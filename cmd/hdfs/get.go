package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
)

func get(args []string) {
	if len(args) == 0 || len(args) > 2 {
		printHelp()
	}

	sources, client, err := normalizePaths(args[0:1])
	if err != nil {
		fatal(err)
	}

	source := sources[0]
	var dest string
	if len(args) == 2 {
		dest = args[1]
	} else {
		cwd, err := os.Getwd()
		if err != nil {
			fatal(err)
		}

		_, name := path.Split(source)
		dest = filepath.Join(cwd, name)
	}

	err = client.Walk(source, func(p string, fi os.FileInfo, err error) error {
		fullDest := filepath.Join(dest, strings.TrimPrefix(p, source))

		exists := os.IsNotExist(err)
		if err != nil && exists {
			fmt.Println(err)
			os.Exit(-1) // file not found
		}

		if fi.IsDir() {
			err = os.Mkdir(fullDest, 0755)
			if err != nil {
				fatal(err)
			}
		} else {
			err = client.CopyToLocal(p, fullDest)
			if pathErr, ok := err.(*os.PathError); ok {
				fatal(pathErr)
			} else if err != nil {
				fatal(err)
			}
		}
		return nil
	})

	if err != nil {
		fatal(err)
	}
}

func getmerge(args []string, addNewlines bool) {
	if len(args) != 2 {
		printHelp()
	}

	dest := args[1]
	sources, client, err := normalizePaths(args[0:1])
	if err != nil {
		fatal(err)
	}

	local, err := os.Create(dest)
	if err != nil {
		fatal(err)
	}

	source := sources[0]
	children, err := client.ReadDir(source)
	if err != nil {
		fatal(err)
	}

	readers := make([]io.Reader, 0, len(children))
	for _, child := range children {
		if child.IsDir() {
			continue
		}

		childPath := path.Join(source, child.Name())
		file, err := client.Open(childPath)
		if err != nil {
			fatal(err)
		}

		readers = append(readers, file)
		if addNewlines {
			readers = append(readers, bytes.NewBufferString("\n"))
		}
	}

	_, err = io.Copy(local, io.MultiReader(readers...))
	if err != nil {
		fatal(err)
	}
}
