package cleaners

import (
	"regexp"
	"strings"
	"fmt"
	"net/http"
	"net/url"
	"encoding/json"
	"io"
	"wxindexer/containers"
)

var (
	reLinkExtract        = regexp.MustCompile(`\[\[([^\|\]]+)`)
	reRefTag             = regexp.MustCompile(`(?s)<ref[^>]*?>.*?</ref>`)
	reSelfClosingRef     = regexp.MustCompile(`(?s)<ref[^>]*/>`)
	reTemplate           = regexp.MustCompile(`(?s)\{\{.*?\}\}`)
	reTable              = regexp.MustCompile(`(?s)\{\|.*?\|\}`)
	reFileLink           = regexp.MustCompile(`\[\[File:[^\]]*\]\]`)
	reImageLink          = regexp.MustCompile(`\[\[Image:[^\]]*\]\]`)
	reCategory           = regexp.MustCompile(`\[\[Category:[^\]]*\]\]`)
	reInternalLink       = regexp.MustCompile(`\[\[([^\|\]]*\|)?([^\]]+)\]\]`)
	reExternalLink       = regexp.MustCompile(`\[(https?://[^\s\]]+)(\s+[^\]]+)?\]`)
	reHTMLComment        = regexp.MustCompile(`(?s)<!--.*?-->`)
	reHTMLTag            = regexp.MustCompile(`</?[a-zA-Z]+.*?>`)
	reBoldItalic         = regexp.MustCompile(`'''''(.*?)'''''`)
	reBold               = regexp.MustCompile(`'''(.*?)'''`)
	reItalic             = regexp.MustCompile(`''(.*?)''`)
	reQuotes             = regexp.MustCompile(`"(.*?)"`)
	reNonAlphanumeric    = regexp.MustCompile(`[^a-zA-Z0-9\s]+`)
	reExtraWhitespace    = regexp.MustCompile(`[ \t]+`)
	reWhitespaceLines    = regexp.MustCompile(`(?m)^[ \t\r\f\v]+$`)
	reMultipleNewlines   = regexp.MustCompile(`\n`)
	reRedirect 			 = regexp.MustCompile(`^#REDIRECT \[\[(.*?)\]\]`)
	invalidPrefixes      = get_invalid_namespaces()
)

type WikipediaCleaner struct {}

func NewWikipediaCleaner() Cleaner {
	var c WikipediaCleaner
	return &c
}

func (v *WikipediaCleaner) Clean(text string) containers.Doc {

	// Check for a redirect page
	if redirect_text := reRedirect.FindStringSubmatch(text); len(redirect_text) > 1 {
		redirect_link := url.PathEscape(strings.ReplaceAll(redirect_text[1], " ", "_"))
		return containers.Doc{Body: nil, Links: nil, Redirect: &redirect_link}
	}

	var links []string
	linkSet := make(map[string]bool)

	entity_replacements := map[string]string{
		"&nbsp;": " ", "&amp;": " ", "&lt;": " ", "&gt;": " ", "&quot;": "",
	}

	// find and save all links
	matches := reLinkExtract.FindAllStringSubmatch(text, -1)
	for _, match := range matches {
		link := strings.TrimSpace(match[1])
		parts := strings.Split(link, ":")
		if link != "" && !linkSet[link] && (len(parts) <= 1 || !invalidPrefixes.Contains(parts[0])){
			link = url.PathEscape(strings.ReplaceAll(link, " ", "_"))
			linkSet[link] = true
			links = append(links, link)
		}
	}

	// remove metadata junk
	text = reRefTag.ReplaceAllString(text, "")
	text = reSelfClosingRef.ReplaceAllString(text, "")
	text = reTemplate.ReplaceAllString(text, "")
	text = reTable.ReplaceAllString(text, "")
	text = reFileLink.ReplaceAllString(text, "")
	text = reImageLink.ReplaceAllString(text, "")
	text = reCategory.ReplaceAllString(text, "")
	text = reInternalLink.ReplaceAllString(text, "$2")
	text = reExternalLink.ReplaceAllString(text, "$2")
	text = reHTMLComment.ReplaceAllString(text, "")
	text = reHTMLTag.ReplaceAllString(text, "")

	// remove formatting
	text = reBoldItalic.ReplaceAllString(text, "$1")
	text = reBold.ReplaceAllString(text, "$1")
	text = reItalic.ReplaceAllString(text, "$1")
	text = reQuotes.ReplaceAllString(text, "$1")

	// Remove unwanted HTML entities
	for k, v := range entity_replacements {
		text = strings.ReplaceAll(text, k, v)
	}

	// Remove any remaining non-alphanumeric characters
	text = reNonAlphanumeric.ReplaceAllString(text, "")

	// Remove excessive whitespace
	text = reExtraWhitespace.ReplaceAllString(text, " ")
	text = reWhitespaceLines.ReplaceAllString(text, "")
	text = reMultipleNewlines.ReplaceAllString(text, "")

	// Lowercase everything
	text = strings.ToLower(text)

	text = strings.TrimSpace(text)

	return containers.Doc{Body: &text, Links: &links, Redirect: nil}
}

func get_invalid_namespaces() *containers.Set[string] {
	fmt.Println("wxindexer/cleaner: fetching invalid namespaces")
	url := "https://en.wikipedia.org/w/api.php?action=query&meta=siteinfo&siprop=namespaces&format=json"

	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		panic(err)
	}

	req.Header.Set("User-Agent", "wxindexer/0.0.0dev3 (https://github.com/MFactor1/windex)") // set a descriptive UA

	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		panic(fmt.Sprintf("unexpected status %d: %s", resp.StatusCode, string(body)))
	}

	var data map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		panic(err)
	}

	query, exists := data["query"].(map[string]any)
	if !exists {
		panic(fmt.Errorf("Expected 'query' key in namespaces JSON: %s", data))
	}

	namespaces, exists := query["namespaces"].(map[string]any)
	if !exists {
		panic(fmt.Errorf("Expected 'namespaces' key in namespaces JSON: %s", query))
	}

	invalid_namespaces := containers.NewSet[string]()
	for _, namespace := range namespaces {
		if nsMap, ok := namespace.(map[string]any); ok {
			if name, exists := nsMap["*"]; exists && name != "" {
				invalid_namespaces.Add(name.(string))
			}
		}
	}

	return invalid_namespaces
}
