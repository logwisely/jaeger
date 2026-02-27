// Copyright (c) 2026 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package attrstore

// Factory can create an attributes Reader.
//
// This is an optional capability: most storage backends will not implement it.
type Factory interface {
	CreateAttributesReader() (Reader, error)
}
