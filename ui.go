package wings

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed ui/*
var uiFiles embed.FS

// uiFS is the embedded web UI, rooted so "/" serves ui/index.html.
func uiFS() fs.FS {
	sub, err := fs.Sub(uiFiles, "ui")
	if err != nil {
		panic(err) // the embed directive guarantees ui/ exists
	}
	return sub
}

func uiStatic() http.Handler { return http.FileServer(http.FS(uiFS())) }
