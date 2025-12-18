package containers

type Set[T comparable] map[T]struct{}

func NewSet[T comparable]() *Set[T] {
	s := make(Set[T])
	return &s
}

func SetFromItem[T comparable](item T) *Set[T] {
	s := make(Set[T])
	s[item] = struct{}{}
	return &s
}

func SetFromSlice[T comparable](slice []T) *Set[T] {
	s := make(Set[T])
	for _, item := range slice {
		s[item] = struct{}{}
	}
	return &s
}

func (s *Set[T]) Add(item T) {
	(*s)[item] = struct{}{}
}

func (s *Set[T]) Contains(item T) bool {
	_, exists := (*s)[item]
	return exists
}

func (s *Set[T]) Remove(item T) {
	delete(*s, item)
}

func (s *Set[T]) Union(other *Set[T]) {
	for item := range *other {
		(*s)[item] = struct{}{}
	}
}

func (s *Set[T]) ToSlice() []T {
	keys := make([]T, 0, len(*s))
	for item := range *s {
		keys = append(keys, item)
	}
	return keys
}
