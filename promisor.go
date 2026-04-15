package git

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/go-git/go-git/v6/config"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/client"
	"github.com/go-git/go-git/v6/plumbing/storer"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/go-git/go-git/v6/storage"
)

// objectFetcher is the interface for fetching missing objects on demand.
type objectFetcher interface {
	FetchObjects(ctx context.Context, hashes []plumbing.Hash) error
}

// promiserStorer wraps a storage.Storer and intercepts object lookups.
// When an object is not found in the inner storer (ErrObjectNotFound),
// it attempts to fetch the missing object from a promisor remote before
// returning the error. This enables partial clone (--filter) support.
//
// Only EncodedObject, HasEncodedObject, and EncodedObjectSize are
// overridden. All other Storer methods delegate to the inner storer
// via embedding. Optional interfaces that both filesystem.Storage and
// memory.Storage implement (PackedObjectStorer, LooseObjectStorer,
// io.Closer) are delegated unconditionally. PackfileWriter is delegated
// conditionally via promiserStorerPW because not all storers implement it.
type promiserStorer struct {
	storage.Storer // delegate all other methods to the inner storer

	fetcher objectFetcher
	mu      sync.Mutex
}

// promiserStorerPW extends promiserStorer with PackfileWriter delegation.
// It is returned by newPromiserStorer when the inner storer implements
// storer.PackfileWriter (e.g. filesystem.Storage).
type promiserStorerPW struct {
	*promiserStorer
	pw storer.PackfileWriter
}

// PackfileWriter delegates to the inner storer's PackfileWriter.
func (s *promiserStorerPW) PackfileWriter() (io.WriteCloser, error) {
	return s.pw.PackfileWriter()
}

// newPromiserStorer wraps inner with on-demand object fetching via fetcher.
// If inner implements storer.PackfileWriter, the returned storer also
// implements it (via promiserStorerPW).
func newPromiserStorer(inner storage.Storer, fetcher objectFetcher) storage.Storer {
	ps := &promiserStorer{
		Storer:  inner,
		fetcher: fetcher,
	}

	if pw, ok := inner.(storer.PackfileWriter); ok {
		return &promiserStorerPW{promiserStorer: ps, pw: pw}
	}
	return ps
}

// getPromiserStorer extracts the *promiserStorer from a storage.Storer,
// handling both promiserStorer and promiserStorerPW.
func getPromiserStorer(s storage.Storer) (*promiserStorer, bool) {
	switch v := s.(type) {
	case *promiserStorer:
		return v, true
	case *promiserStorerPW:
		return v.promiserStorer, true
	}
	return nil, false
}

// EncodedObject returns an object from the storer. If the object is not
// found and its type is a blob (or AnyObject), an on-demand fetch from
// the promisor remote is attempted. Trees and commits are not fetched
// because blob:none filters only exclude blobs.
func (s *promiserStorer) EncodedObject(t plumbing.ObjectType, h plumbing.Hash) (plumbing.EncodedObject, error) {
	obj, err := s.Storer.EncodedObject(t, h)
	if err != nil && isObjectNotFound(err) && shouldFetch(t) {
		fetchErr := s.fetchObject(h)
		if fetchErr == nil {
			return s.Storer.EncodedObject(t, h)
		}
		return nil, errors.Join(err, fetchErr)
	}
	return obj, err
}

// HasEncodedObject checks whether the object exists. If not found, an
// on-demand fetch is attempted.
func (s *promiserStorer) HasEncodedObject(h plumbing.Hash) error {
	err := s.Storer.HasEncodedObject(h)
	if err != nil && isObjectNotFound(err) {
		fetchErr := s.fetchObject(h)
		if fetchErr == nil {
			return s.Storer.HasEncodedObject(h)
		}
		return errors.Join(err, fetchErr)
	}
	return err
}

// EncodedObjectSize returns the size of an object. If not found, an
// on-demand fetch is attempted.
func (s *promiserStorer) EncodedObjectSize(h plumbing.Hash) (int64, error) {
	sz, err := s.Storer.EncodedObjectSize(h)
	if err != nil && isObjectNotFound(err) {
		fetchErr := s.fetchObject(h)
		if fetchErr == nil {
			return s.Storer.EncodedObjectSize(h)
		}
		return 0, errors.Join(err, fetchErr)
	}
	return sz, err
}

// fetchObject fetches a single object from the promisor remote.
// Access is serialized with a mutex to avoid duplicate concurrent fetches.
func (s *promiserStorer) fetchObject(h plumbing.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Re-check after acquiring the lock; another goroutine may have
	// fetched the object while we were waiting.
	if err := s.Storer.HasEncodedObject(h); err == nil {
		return nil
	}

	return s.fetcher.FetchObjects(context.Background(), []plumbing.Hash{h})
}

// Close delegates to the inner storer's Close if it implements io.Closer.
func (s *promiserStorer) Close() error {
	if c, ok := s.Storer.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

// ObjectPacks delegates to the inner storer's PackedObjectStorer.
func (s *promiserStorer) ObjectPacks() ([]plumbing.Hash, error) {
	if pos, ok := s.Storer.(storer.PackedObjectStorer); ok {
		return pos.ObjectPacks()
	}
	return nil, plumbing.ErrObjectNotFound
}

// DeleteOldObjectPackAndIndex delegates to the inner storer's PackedObjectStorer.
func (s *promiserStorer) DeleteOldObjectPackAndIndex(h plumbing.Hash, t time.Time) error {
	if pos, ok := s.Storer.(storer.PackedObjectStorer); ok {
		return pos.DeleteOldObjectPackAndIndex(h, t)
	}
	return transport.ErrPackedObjectsNotSupported
}

// ForEachObjectHash delegates to the inner storer's LooseObjectStorer.
func (s *promiserStorer) ForEachObjectHash(f func(plumbing.Hash) error) error {
	if los, ok := s.Storer.(storer.LooseObjectStorer); ok {
		return los.ForEachObjectHash(f)
	}
	return ErrLooseObjectsNotSupported
}

// LooseObjectTime delegates to the inner storer's LooseObjectStorer.
func (s *promiserStorer) LooseObjectTime(h plumbing.Hash) (time.Time, error) {
	if los, ok := s.Storer.(storer.LooseObjectStorer); ok {
		return los.LooseObjectTime(h)
	}
	return time.Time{}, plumbing.ErrObjectNotFound
}

// DeleteLooseObject delegates to the inner storer's LooseObjectStorer.
func (s *promiserStorer) DeleteLooseObject(h plumbing.Hash) error {
	if los, ok := s.Storer.(storer.LooseObjectStorer); ok {
		return los.DeleteLooseObject(h)
	}
	return ErrLooseObjectsNotSupported
}

// promiserObjectFetcher fetches missing objects from a promisor remote.
// It uses the inner storer directly (not the promiserStorer wrapper) to
// avoid infinite recursion when writing received packfiles.
type promiserObjectFetcher struct {
	remoteName    string
	inner         storage.Storer  // unwrapped storer for writing fetched objects
	clientOptions []client.Option // transport options for on-demand fetch
}

// FetchObjects fetches the given objects from the promisor remote in a
// single request. This corresponds to git's promisor_remote_get_direct(),
// which runs: git fetch <remote> --no-tags --no-write-fetch-head --stdin
// with the object IDs piped to stdin.
func (f *promiserObjectFetcher) FetchObjects(ctx context.Context, hashes []plumbing.Hash) error {
	if len(hashes) == 0 {
		return nil
	}

	cfg, err := f.inner.Config()
	if err != nil {
		return fmt.Errorf("promisor fetch: read config: %w", err)
	}

	rc, ok := cfg.Remotes[f.remoteName]
	if !ok || len(rc.URLs) == 0 {
		return fmt.Errorf("promisor fetch: remote %q not found or has no URLs", f.remoteName)
	}

	cl, req, err := newClient(rc.URLs[0], f.clientOptions)
	if err != nil {
		return fmt.Errorf("promisor fetch: new client: %w", err)
	}

	req.Command = transport.UploadPackService
	sess, err := cl.Handshake(ctx, req)
	if err != nil {
		return fmt.Errorf("promisor fetch: handshake: %w", err)
	}

	fetchReq := &transport.FetchRequest{
		Wants: hashes,
	}

	if err := sess.Fetch(ctx, f.inner, fetchReq); err != nil {
		_ = sess.Close()
		return fmt.Errorf("promisor fetch: %w", err)
	}

	if err := sess.Close(); err != nil {
		return fmt.Errorf("promisor fetch: close: %w", err)
	}

	return nil
}

// findPromisorRemote returns the name of the promisor remote from the config.
// It first checks for remote.<name>.promisor = true (modern git),
// then falls back to extensions.partialClone (legacy).
// Returns empty string if no promisor remote is configured.
//
// NOTE: when multiple remotes have Promisor=true, selection is
// non-deterministic because cfg.Remotes is a map. This matches the
// common single-promisor-remote case; deterministic multi-remote
// ordering would require preserving config-file order.
func findPromisorRemote(cfg *config.Config) string {
	for name, rc := range cfg.Remotes {
		if rc.Promisor {
			return name
		}
	}

	// Legacy: extensions.partialClone = <remote-name>
	if cfg.Extensions.PartialClone != "" {
		return cfg.Extensions.PartialClone
	}

	return ""
}

// promisorOptions holds transport options for on-demand object fetching.
type promisorOptions struct {
	ClientOptions []client.Option
}

// wrapStorerIfPromisor reads the config from s and, if a promisor remote is
// configured, wraps s with a promiserStorer for on-demand object fetching.
func wrapStorerIfPromisor(s storage.Storer, opts *promisorOptions) storage.Storer {
	cfg, err := s.Config()
	if err != nil {
		return s
	}

	remoteName := findPromisorRemote(cfg)
	if remoteName == "" {
		return s
	}

	fetcher := &promiserObjectFetcher{
		remoteName: remoteName,
		inner:      s,
	}
	if opts != nil {
		fetcher.clientOptions = opts.ClientOptions
	}
	return newPromiserStorer(s, fetcher)
}

// isObjectNotFound reports whether err indicates that an object was not found.
func isObjectNotFound(err error) bool {
	return errors.Is(err, plumbing.ErrObjectNotFound)
}

// shouldFetch reports whether on-demand fetching should be attempted for
// the given object type. Only blobs (and AnyObject, since we don't know
// the type yet) are fetched on demand. Trees and commits are always
// received during clone/fetch with blob:none.
func shouldFetch(t plumbing.ObjectType) bool {
	return t == plumbing.BlobObject || t == plumbing.AnyObject
}
