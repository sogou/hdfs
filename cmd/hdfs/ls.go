package main

import (
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/colinmarc/hdfs/v2"
	"github.com/colinmarc/hdfs/v2/hadoopconf"
)

func ls(paths []string, long, all, humanReadable bool) {

	if len(paths) == 0 {
		fatal("path is empty")
	}

	for _, perPath := range paths {
		files := make([]string, 0, len(paths))
		fileInfos := make([]os.FileInfo, 0, len(paths))
		dirs := make([]string, 0, len(paths))

		if strings.Compare(perPath, "/") == 0 {
			conf, exception := hadoopconf.LoadFromEnvironment()
			if exception != nil {
				fatal("NormalizePaths occur problem loading configuration: ", exception)
			}

			for k, v := range conf {
				if strings.Contains(k, "fs.viewfs.mounttable") {
					u, err := url.Parse(v)
					if err == nil && strings.Count(u.Path, "/") == 1 {
						dirs = append(dirs, u.Path)
					}
				}
			}

			for _, dir := range dirs {
				fmt.Println(dir)
			}
			continue
		}

		abPath, client, err := getClientAndExpandedPaths([]string{perPath})

		if err != nil {
			fatal(err)
		}
		fi, err := client.Stat(abPath[0])

		if err != nil {
			fatal(err)
		}

		if fi.IsDir() {
			dirs = append(dirs, perPath)
		} else {
			files = append(files, perPath)
			fileInfos = append(fileInfos, fi)
		}

		if long {
			tw := lsTabWriter()
			for i, p := range files {
				printLong(tw, p, fileInfos[i], humanReadable)
			}

			tw.Flush()
		} else {
			for _, p := range files {
				fmt.Println(p)
			}
		}

		for i, dir := range abPath {
			if i > 0 {
				fmt.Println()
			}

			fmt.Printf("%s/:\n", dir)
			printDir(client, dir, long, all, humanReadable)
		}
	}
}

func printDir(client *hdfs.Client, dir string, long, all, humanReadable bool) {
	dirReader, err := client.Open(dir)
	if err != nil {
		fatal(err)
	}

	var tw *tabwriter.Writer
	if long {
		tw = lsTabWriter()
		defer tw.Flush()
	}

	if all {
		if long {
			dirInfo, err := client.Stat(dir)
			if err != nil {
				fatal(err)
			}

			parentPath := path.Join(dir, "..")
			parentInfo, err := client.Stat(parentPath)
			if err != nil {
				fatal(err)
			}

			printLong(tw, ".", dirInfo, humanReadable)
			printLong(tw, "..", parentInfo, humanReadable)
		} else {
			fmt.Println(".")
			fmt.Println("..")
		}
	}

	var partial []os.FileInfo
	for ; err != io.EOF; partial, err = dirReader.Readdir(100) {
		if err != nil {
			fatal(err)
		}

		printFiles(tw, partial, long, all, humanReadable)
	}

	if long {
		tw.Flush()
	}
}

func printFiles(tw *tabwriter.Writer, files []os.FileInfo, long, all, humanReadable bool) {
	for _, file := range files {
		if !all && strings.HasPrefix(file.Name(), ".") {
			continue
		}

		if long {
			printLong(tw, file.Name(), file, humanReadable)
		} else {
			fmt.Println(file.Name())
		}
	}
}

func printLong(tw *tabwriter.Writer, name string, info os.FileInfo, humanReadable bool) {
	fi := info.(*hdfs.FileInfo)
	// mode owner group size date(\w tab) time/year name
	mode := fi.Mode().String()
	owner := fi.Owner()
	group := fi.OwnerGroup()
	size := strconv.FormatInt(fi.Size(), 10)
	if humanReadable {
		size = formatBytes(uint64(fi.Size()))
	}

	modtime := fi.ModTime()
	date := modtime.Format("Jan _2")
	var timeOrYear string
	if modtime.Year() == time.Now().Year() {
		timeOrYear = modtime.Format("15:04")
	} else {
		timeOrYear = modtime.Format("2006")
	}

	fmt.Fprintf(tw, "%s \t%s \t %s \t %s \t%s \t%s \t%s\n",
		mode, owner, group, size, date, timeOrYear, name)
}

func lsTabWriter() *tabwriter.Writer {
	return tabwriter.NewWriter(os.Stdout, 3, 8, 0, ' ', tabwriter.AlignRight|tabwriter.TabIndent)
}
