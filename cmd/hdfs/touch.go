package main

import (
	"os"
	"time"
)

func touch(paths []string, noCreate bool) {
	paths, client, err := normalizePaths(paths)
	if err != nil {
		fatal(err)
	}

	if len(paths) == 0 {
		printHelp()
	}

	for _, p := range paths {
		if hasGlob(p) {
			fatal(&os.PathError{"mkdir", p, os.ErrNotExist})
		}

		_, err := client.Stat(p)
		exists := !os.IsNotExist(err)
		if (err != nil && exists) || (!exists && noCreate) {
			fatal(err)
		}

		if exists {
			now := time.Now()
			mtime := now
			atime := now

			err = client.Chtimes(p, mtime, atime)
		} else {
			err = client.CreateEmptyFile(p)
		}

		if err != nil {
			fatal(err)
		}
	}
}
