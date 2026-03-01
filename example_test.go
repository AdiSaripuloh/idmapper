package idmapper_test

import (
	"fmt"

	"github.com/AdiSaripuloh/idmapper"
)

func ExampleNewRWMutex() {
	m := idmapper.NewRWMutex([]string{"alice", "bob", "carol"})

	fmt.Println(m.Set("dave"))  // new key
	fmt.Println(m.Set("alice")) // existing key

	id, ok := m.Get("bob")
	fmt.Println(id, ok)

	id, ok = m.Get("unknown")
	fmt.Println(id, ok)

	// Output:
	// 4
	// 1
	// 2 true
	// 0 false
}

func ExampleNewFreeze() {
	m := idmapper.NewFreeze([]string{"alice", "bob", "carol"})
	m.Set("dave")
	m.Freeze()

	id, ok := m.Get("alice")
	fmt.Println(id, ok)

	id, ok = m.Get("dave")
	fmt.Println(id, ok)

	ids := m.Gets([]string{"carol", "bob"})
	fmt.Println(ids)

	// Output:
	// 1 true
	// 4 true
	// [3 2]
}

func ExampleNewCOW() {
	m := idmapper.NewCOW([]string{"alice", "bob", "carol"})

	fmt.Println(m.Set("dave"))

	id, ok := m.Get("alice")
	fmt.Println(id, ok)

	snap := m.GetSnapshot()
	fmt.Println(snap["bob"])

	// Output:
	// 4
	// 1 true
	// 2
}

func ExampleRWMutex_Sets() {
	m := idmapper.NewRWMutex([]string{"alice", "bob"})
	ids := m.Sets([]string{"carol", "alice", "dave"})
	fmt.Println(ids)

	// Output:
	// [3 1 4]
}

func ExampleRWMutex_Gets() {
	m := idmapper.NewRWMutex([]string{"alice", "bob", "carol"})
	ids := m.Gets([]string{"bob", "unknown", "alice"})
	fmt.Println(ids)

	// Output:
	// [2 0 1]
}

func ExampleRWMutex_Len() {
	m := idmapper.NewRWMutex([]string{"alice", "bob", "carol"})
	fmt.Println(m.Len())

	m.Set("dave")
	fmt.Println(m.Len())

	// Output:
	// 3
	// 4
}

func ExampleFreeze_Sets() {
	m := idmapper.NewFreeze[string](nil)
	ids := m.Sets([]string{"alice", "bob", "carol"})
	fmt.Println(ids)
	m.Freeze()

	// Output:
	// [1 2 3]
}

func ExampleCOW_Sets() {
	m := idmapper.NewCOW([]string{"alice", "bob"})
	ids := m.Sets([]string{"carol", "alice", "dave"})
	fmt.Println(ids)

	// Output:
	// [3 1 4]
}

func ExampleCOW_Len() {
	m := idmapper.NewCOW([]string{"alice", "bob", "carol"})
	fmt.Println(m.Len())

	// Output:
	// 3
}

func ExampleFreeze_Gets() {
	m := idmapper.NewFreeze([]string{"alice", "bob", "carol"})
	m.Freeze()

	ids := m.Gets([]string{"carol", "unknown", "alice"})
	fmt.Println(ids)

	// Output:
	// [3 0 1]
}

func ExampleFreeze_Len() {
	m := idmapper.NewFreeze([]string{"alice", "bob", "carol"})
	fmt.Println(m.Len())

	// Output:
	// 3
}

func ExampleCOW_Gets() {
	m := idmapper.NewCOW([]string{"alice", "bob", "carol"})
	ids := m.Gets([]string{"bob", "unknown", "alice"})
	fmt.Println(ids)

	// Output:
	// [2 0 1]
}

func ExampleMPHF_Gets() {
	m := idmapper.NewMPHF([]string{"alice", "bob", "carol"}, idmapper.StringHasher)
	m.Freeze()

	ids := m.Gets([]string{"carol", "unknown", "alice"})
	fmt.Println(ids)

	// Output:
	// [3 0 1]
}

func ExampleMPHF_Len() {
	m := idmapper.NewMPHF([]string{"alice", "bob", "carol"}, idmapper.StringHasher)
	m.Freeze()
	fmt.Println(m.Len())

	// Output:
	// 3
}

func ExampleNewMPHF() {
	m := idmapper.NewMPHF([]string{"alice", "bob", "carol"}, idmapper.StringHasher)
	m.Set("dave")
	m.Freeze()

	id, ok := m.Get("alice")
	fmt.Println(id, ok)

	id, ok = m.Get("dave")
	fmt.Println(id, ok)

	id, ok = m.Get("unknown")
	fmt.Println(id, ok)

	// Output:
	// 1 true
	// 4 true
	// 0 false
}
