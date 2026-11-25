package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

var layersBottomUp = []string{"dst", "shared", "controlplane", "broker"}

func checkModuleLayer(root string) []diagnostic {
	rank := map[string]int{}
	for i, m := range layersBottomUp {
		rank[m] = i
	}
	prefix := strings.TrimSuffix(mdsPrefix, "/")

	var diags []diagnostic
	for _, module := range layersBottomUp {
		gomod := filepath.Join(root, module, "go.mod")
		reqs, err := requiredModulePaths(gomod)
		if err != nil {
			diags = append(diags, diagnostic{
				lint:    "module-layer",
				key:     module + "/go.mod",
				message: fmt.Sprintf("cannot read %s/go.mod: %v", module, err),
				file:    module + "/go.mod",
				line:    1,
			})
			continue
		}
		myRank := rank[module]
		for _, path := range reqs {
			if !strings.HasPrefix(path, prefix+"/") {
				continue
			}
			required := strings.TrimPrefix(path, prefix+"/")
			reqRank, ok := rank[required]
			if !ok {
				continue
			}
			if reqRank > myRank {
				var pic strings.Builder
				fmt.Fprintf(&pic, "    layer: %s\n", strings.Join(layersBottomUp, " <- "))
				fmt.Fprintf(&pic, "    %s requires %s (rightward, forbidden)\n", module, required)
				diags = append(diags, diagnostic{
					lint:    "module-layer",
					key:     module + "->" + required,
					message: fmt.Sprintf("`%s` must not depend on `%s`", module, required),
					file:    module + "/go.mod",
					line:    1,
					notes: []string{
						fmt.Sprintf("found require %s in %s/go.mod", path, module),
						"order: dst <- shared <- controlplane <- broker",
					},
					picture: pic.String(),
				})
			}
		}
	}
	return diags
}

func requiredModulePaths(gomod string) ([]string, error) {
	f, err := os.Open(gomod)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var out []string
	inBlock := false
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if i := strings.Index(line, "//"); i >= 0 {
			line = strings.TrimSpace(line[:i])
		}
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "require") && strings.HasSuffix(line, "(") {
			inBlock = true
			continue
		}
		if inBlock {
			if line == ")" {
				inBlock = false
				continue
			}
			fields := strings.Fields(line)
			if len(fields) >= 1 {
				out = append(out, fields[0])
			}
			continue
		}
		if strings.HasPrefix(line, "require ") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				out = append(out, fields[1])
			}
		}
	}
	return out, sc.Err()
}
