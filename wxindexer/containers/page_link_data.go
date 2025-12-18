package containers

type PageLinkData struct {
	URL string
	Links *Set[string]
	Redirect *string
}
