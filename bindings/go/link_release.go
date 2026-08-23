//go:build !prolly_dev

package prolly

/*
#cgo darwin LDFLAGS: -lprolly_bindings
#cgo linux LDFLAGS: -lprolly_bindings
#cgo windows LDFLAGS: -lprolly_bindings
*/
import "C"
