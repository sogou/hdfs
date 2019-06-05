package main

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"regexp"
	"strings"

	"github.com/colinmarc/hdfs/v2"
	"github.com/colinmarc/hdfs/v2/hadoopconf"
)

var (
	errMultipleNamenodeUrls = errors.New("Multiple namenode URLs specified")
)

func userDir(client *hdfs.Client) string {
	return path.Join("/user", client.User())
}

// normalizePaths parses the hosts out of HDFS URLs, and turns relative paths
// into absolute ones (by appending /user/<user>). If multiple HDFS urls with
// differing hosts are passed in, it returns an error.
func normalizePaths(paths []string) ([]string, *hdfs.Client, error) {
	namenode := ""
	cleanPaths := make([]string, 0, len(paths))

	for _, rawurl := range paths {
		url, err := url.Parse(rawurl)
		if err != nil {
			return nil, nil, err
		}

		if url.Host != "" {
			if namenode != "" && namenode != url.Host {
				return nil, nil, errMultipleNamenodeUrls
			}

			namenode = url.Host
		}
		cleanPaths = append(cleanPaths, path.Clean(url.Path))
	}
	if namenode != "" {
		client, err := getActiveNameNode(namenode, cleanPaths[0])
		return cleanPaths, client, err
	}

	conf, err := hadoopconf.LoadFromEnvironment()
	if err != nil {
		return cleanPaths, nil, fmt.Errorf("NormalizePaths occur problem loading configuration: %s", err)
	}

	pathsArray := strings.Split(cleanPaths[0], "/")
	firstLevelPath := "/" + pathsArray[1]
	var secondPath string
	var isFirstLevelPath bool
	prefixPath := "fs.viewfs.mounttable.nsX.link."
	prefixNNX := "dfs.ha.namenodes."
	prefixNode := "dfs.namenode.rpc-address."

	if len(pathsArray) > 2 && pathsArray[2] != "" {
		secondPath = firstLevelPath + "/" + pathsArray[2]
	}
	node := conf[prefixPath+secondPath]
	if node == "" {
		isFirstLevelPath = true
		node = conf[prefixPath+firstLevelPath]
	}
	u, err := url.Parse(node)
	if err != nil {
		return nil, nil, err
	}
	nnxArray := strings.Split(conf[prefixNNX+u.Host], ",")
	for _, nni := range nnxArray {
		tmp := conf[prefixNode+u.Host+"."+nni]
		namenode += tmp
		namenode += ","
	}
	namenode = strings.Trim(namenode, ",")

	// namenode contains 2 value, eg: rsync.master003.sunshine.hadoop.js.ted:8020,rsync.master004.sunshine.hadoop.js.ted:8020
	var existPath string
	if isFirstLevelPath {
		existPath = firstLevelPath
	} else {
		existPath = cleanPaths[0][0: strings.LastIndex(cleanPaths[0], "/")]
	}
	activennCli, err := getActiveNameNode(namenode, existPath)

	return cleanPaths, activennCli, err
}

func getActiveNameNode(nn string, path string) (*hdfs.Client, error) {
	namenodeArray := strings.Split(nn, ",")
	var client *hdfs.Client
	var err error
	var node string

	for _, node = range namenodeArray {
		client, err = getClient(node)
		if err != nil {
			continue
		}
		_, err = client.Stat(path)
		if err == nil {
			break
		}
	}
	if err != nil {
		return nil, err
	}
	return client, err
}

func getClientAndExpandedPaths(paths []string) ([]string, *hdfs.Client, error) {

	paths, nnClient, err := normalizePaths(paths)
	if err != nil {
		return nil, nil, err
	}

	expanded, err := expandPaths(nnClient, paths)
	if err != nil {
		return nil, nil, err
	}

	return expanded, nnClient, nil
}

// TODO: not really sure checking for a leading \ is the way to test for
// escapedness.
func hasGlob(fragment string) bool {
	match, _ := regexp.MatchString(`([^\\]|^)[[*?]`, fragment)
	return match
}

// expandGlobs recursively expands globs in a filepath. It assumes the paths
// are already cleaned and normalized (ie, absolute).
func expandGlobs(client *hdfs.Client, globbedPath string) ([]string, error) {
	parts := strings.Split(globbedPath, "/")[1:]
	var res []string
	var splitAt int

	for splitAt = range parts {
		if hasGlob(parts[splitAt]) {
			break
		}
	}

	var base, glob, next, remainder string
	base = "/" + path.Join(parts[:splitAt]...)
	glob = parts[splitAt]

	if len(parts) > splitAt+1 {
		next = parts[splitAt+1]
		remainder = path.Join(parts[splitAt+2:]...)
	} else {
		next = ""
		remainder = ""
	}

	list, err := client.ReadDir(base)
	if err != nil {
		return nil, err
	}

	for _, fi := range list {
		match, _ := path.Match(glob, fi.Name())
		if !match {
			continue
		}

		newPath := path.Join(base, fi.Name(), next, remainder)
		if hasGlob(newPath) {
			if fi.IsDir() {
				children, err := expandGlobs(client, newPath)
				if err != nil {
					return nil, err
				}

				res = append(res, children...)
			}
		} else {
			_, err := client.Stat(newPath)
			if os.IsNotExist(err) {
				continue
			} else if err != nil {
				return nil, err
			}

			res = append(res, newPath)
		}
	}

	return res, nil
}

func expandPaths(client *hdfs.Client, paths []string) ([]string, error) {
	var res []string
	home := userDir(client)

	for _, p := range paths {
		if !path.IsAbs(p) {
			p = path.Join(home, p)
		}

		if hasGlob(p) {
			expanded, err := expandGlobs(client, p)
			if err != nil {
				return nil, err
			} else if len(expanded) == 0 {
				// Fake a PathError for consistency.
				return nil, &os.PathError{"stat", p, os.ErrNotExist}
			}

			res = append(res, expanded...)
		} else {
			res = append(res, p)
		}
	}

	return res, nil
}
