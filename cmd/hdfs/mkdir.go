package main

import (
	"os"
)

func mkdir(paths []string, all bool) {

	if len(paths) == 0 {
		printHelp()
	}

	for _, p := range paths {
		_, client, err := normalizePaths([]string{p})
		if err != nil {
			fatal(err)
		}

		if hasGlob(p) {
			fatal(&os.PathError{"mkdir", p, os.ErrNotExist})
		}

		var mode = 0755 | os.ModeDir
		if all {
			err = client.MkdirAll(p, mode)
		} else {
			err = client.Mkdir(p, mode)
		}

		if err != nil && !(all && os.IsExist(err)) {
			fatal(err)
		}
	}
}
