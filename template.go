package main

import (
	"fmt"
	"net/http"
	"regexp"
	"strings"
)

func getPageNumber(pageName string) string {
	idx := strings.Index(pageName, "-")
	if idx == -1 || idx == len(pageName)-1 {
		return ""
	}
	return pageName[idx+1:]
}

func isPageValid(pageName string) bool {
	var validPageName = regexp.MustCompile(`^page-[1-9][0-9]*$`)
	if pageName == "" {
		return false
	}
	if !validPageName.MatchString(pageName) {
		return false
	}
	return true
}

func pageHandler(w http.ResponseWriter, r *http.Request) {
	pageName := strings.TrimPrefix(r.URL.Path, "/")
	if !isPageValid(pageName) {
		http.NotFound(w, r)
		return
	}

	fmt.Printf("Requested for [%s]\n", pageName)

	templateData := map[string]any{
		"pageNumber": getPageNumber(pageName),
	}

	err := templates.ExecuteTemplate(w, "page.html", templateData)

	if err != nil {
		http.Error(w, "Template error", http.StatusInternalServerError)
	}
}
