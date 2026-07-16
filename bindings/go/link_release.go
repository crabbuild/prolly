//go:build prolly_release

package prolly

/*
#cgo darwin LDFLAGS: -L${SRCDIR}/../../target/release -Wl,-rpath,${SRCDIR}/../../target/release -lprolly_bindings
#cgo linux LDFLAGS: -L${SRCDIR}/../../target/release -Wl,-rpath,${SRCDIR}/../../target/release -lprolly_bindings
#cgo windows LDFLAGS: -L${SRCDIR}/../../target/release -lprolly_bindings
*/
import "C"
