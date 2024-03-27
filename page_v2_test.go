package goparquet

import (
	"bytes"
	"io"
	"testing"

	"github.com/fraugster/parquet-go/parquet"
	"github.com/stretchr/testify/require"
)

func TestDataReaderV2_Read(t *testing.T) {
	pageHeader := &parquet.PageHeader{
		CompressedPageSize:   3,
		UncompressedPageSize: 3,
		DataPageHeaderV2: &parquet.DataPageHeaderV2{
			NumValues:                  5,
			RepetitionLevelsByteLength: 0,
			DefinitionLevelsByteLength: 0,
			NumRows:                    5,
			Encoding:                   parquet.Encoding_PLAIN,
			IsCompressed:               false,
		},
	}

	mockReader := bytes.NewReader([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
	mockDecoder := &mockValuesDecoder{}

	// Create the data page (v2) reader
	reader := &dataPageReaderV2{
		ph:          pageHeader,
		alloc:       newAllocTracker(100),
		valuesCount: 5,
		position:    0,
		fn: func(parquet.Encoding) (valuesDecoder, error) {
			return mockDecoder, nil
		},
	}

	err := reader.read(mockReader, pageHeader, parquet.CompressionCodec_SNAPPY, false)
	require.NoError(t, err)
}

type mockValuesDecoder struct {
	r io.Reader
}

func (m *mockValuesDecoder) init(reader io.Reader) error {
	m.r = reader
	return nil
}

func (m *mockValuesDecoder) decodeValues(_ []interface{}) (int, error) {
	// We don't need to implement this
	return 0, nil
}
