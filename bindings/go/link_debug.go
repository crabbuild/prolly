//go:build !prolly_release

package prolly

/*
#cgo darwin LDFLAGS: -L${SRCDIR}/../../target/debug -Wl,-rpath,${SRCDIR}/../../target/debug -lprolly_bindings
#cgo linux LDFLAGS: -L${SRCDIR}/../../target/debug -Wl,-rpath,${SRCDIR}/../../target/debug -lprolly_bindings
#cgo windows LDFLAGS: -L${SRCDIR}/../../target/debug -lprolly_bindings
*/
import "C"
