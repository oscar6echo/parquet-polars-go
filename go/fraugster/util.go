package pqtest

import "github.com/fraugster/parquet-go/floor/interfaces"

func extractListInt(obj interfaces.UnmarshalObject, fieldName string) ([]int64, error) {
	empty := []int64{}

	list, err := obj.GetField(fieldName).List()
	if err != nil {
		return empty, err
	}

	arr := make([]int64, 0)
	for list.Next() {
		v1, e1 := list.Value()
		if e1 != nil {
			return empty, e1
		}
		v2, e2 := v1.Int64()
		if e2 != nil {
			return empty, e2
		}
		arr = append(arr, v2)
	}

	return arr, nil
}

func extractListFloat(obj interfaces.UnmarshalObject, fieldName string) ([]float64, error) {
	empty := []float64{}

	list, err := obj.GetField(fieldName).List()
	if err != nil {
		return empty, err
	}

	arr := make([]float64, 0)
	for list.Next() {
		v1, e1 := list.Value()
		if e1 != nil {
			return empty, e1
		}
		v2, e2 := v1.Float64()
		if e2 != nil {
			return empty, e2
		}
		arr = append(arr, v2)
	}

	return arr, nil
}

func extractListString(obj interfaces.UnmarshalObject, fieldName string) ([]string, error) {
	empty := []string{}

	list, err := obj.GetField(fieldName).List()
	if err != nil {
		return empty, err
	}

	arr := make([]string, 0)
	for list.Next() {
		v1, e1 := list.Value()
		if e1 != nil {
			return empty, e1
		}
		v2, e2 := v1.ByteArray()
		if e2 != nil {
			return empty, e2
		}
		arr = append(arr, string(v2))
	}

	return arr, nil
}
