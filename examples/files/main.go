// File storage: upload, bind a file to a row, a signed link, a resumable
// upload with progress, and cleanup.
//
//	go run ./examples/files
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	azora "github.com/azora-db/azora-go"
	"github.com/azora-db/azora-go/examples/internal/demo"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// The one URL is also where files are: nothing else to configure.
	db, err := azora.Connect(ctx, demo.Env("AZORA_URL"), azora.WithToken(demo.Env("AZORA_TOKEN")))
	demo.Check(err)
	defer db.Close()
	files := db.Storage()

	// Upload a file and reference it from a row: the FILE column holds
	// the object's id.
	invoice, err := files.Upload(ctx, strings.NewReader("Invoice 2026-0042\nStanding desk frame  129.90\n"), azora.UploadOptions{
		Path: fmt.Sprintf("invoices/2026-0042-%d.txt", time.Now().UnixNano()), ContentType: "text/plain"})
	demo.Check(err)
	res, err := db.Exec(ctx, "INSERT INTO documents(title, file) VALUES (?, ?)", "Invoice 2026-0042", invoice.ID)
	demo.Check(err)
	fmt.Printf("stored %s (%d bytes, sha256 %s…) for document %d\n", invoice.Name, invoice.Size, invoice.SHA256[:12], res.LastInsertID)

	// A signed link opens that one file, without credentials, until it
	// expires.
	link, err := files.SignURL(ctx, invoice.ID, azora.SignOptions{TTL: 5 * time.Minute, Download: true, Filename: "invoice.txt"})
	demo.Check(err)
	resp, err := http.Get(link.URL)
	demo.Check(err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	first, _, _ := strings.Cut(string(body), "\n")
	fmt.Printf("signed link valid until %s: %q\n", link.ExpiresAt.Format(time.RFC3339), first)

	// A large file goes up in chunks. If the connection drops, the upload
	// continues from what the server already holds.
	big := make([]byte, 6<<20)
	_, _ = rand.Read(big)
	sum := sha256.Sum256(big)
	obj, err := files.UploadResumable(ctx, bytes.NewReader(big), int64(len(big)),
		azora.UploadOptions{Name: "backup.bin", SHA256: hex.EncodeToString(sum[:])},
		azora.ResumableOptions{ChunkSize: 2 << 20, OnProgress: func(sent, total int64) {
			fmt.Printf("  uploaded %d of %d MiB\n", sent>>20, total>>20)
		}})
	demo.Check(err)
	fmt.Printf("resumable upload complete: %s, hash verified by the server: %v\n", obj.ID, obj.SHA256 == hex.EncodeToString(sum[:]))

	// Read it back; the server checks the content while sending it.
	var back bytes.Buffer
	n, err := files.DownloadTo(ctx, obj.ID, &back)
	demo.Check(err)
	fmt.Printf("downloaded %d bytes, identical: %v\n", n, bytes.Equal(back.Bytes(), big))

	// A file a row references is protected from deletion.
	err = files.Delete(ctx, invoice.ID)
	fmt.Printf("deleting a referenced file: conflict = %v\n", azora.IsConflict(err))

	// Cleanup: the unreferenced file directly, the referenced one once
	// its row is gone.
	demo.Check(files.Delete(ctx, obj.ID))
	_, err = db.Exec(ctx, "DELETE FROM documents WHERE id = ?", res.LastInsertID)
	demo.Check(err)
	if err := files.Delete(ctx, invoice.ID); err != nil && !azora.IsNotFound(err) {
		demo.Check(err)
	}
	fmt.Println("cleaned up")
}
