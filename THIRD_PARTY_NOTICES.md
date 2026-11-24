# Third-party notices

## Search syncer (Linux amd64)

The `compos-search-syncer` dependency set, as resolved from the root Go module for Linux amd64 with CGO disabled, includes the following modules. Their license texts are reproduced from the corresponding versioned Go module cache entries.

This notice covers only these Go modules. It is not a complete notice for the Compos server, the exporter, the Go toolchain, or container base images and operating-system packages. Release packaging must provide all applicable notices and license texts for each distributed artifact.

### github.com/andybalholm/brotli v1.1.1

Copyright (c) 2009, 2010, 2013-2016 by the Brotli Authors.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

### github.com/golang-jwt/jwt/v5 v5.3.1

Copyright (c) 2012 Dave Grijalva
Copyright (c) 2021 golang-jwt maintainers

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.


### github.com/meilisearch/meilisearch-go v0.36.3

MIT License

Copyright (c) 2020-2025 Meili SAS

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

## Additional Linux binary notices

The Linux amd64 server dependency graph (CGO enabled) and search-syncer dependency graph (CGO disabled) were obtained with `go list -deps` from the root module using Go 1.27.4. Selected versioned module license, copyright, patent and NOTICE files, and Go license and patent text are reproduced in [THIRD_PARTY_LICENSES.txt](THIRD_PARTY_LICENSES.txt). The bundle omits Rhiza's MIT notices under separate permission granted for Compos binary distributions. This permission does not cover other third-party notices or other forms of distribution. Some included nested texts may apply only to unused upstream files. The dependency graph may differ on other architectures or with other build settings; update and review the notices for each release.

### SQLite translation and supporting code

The server imports the core and FTS5 packages from `github.com/ncruces/go-sqlite3-wasm/v3` v3.2.35304. Its [build scripts](https://github.com/ncruces/go-sqlite3-wasm/tree/v3.2.35304/build) identify SQLite 3.53.4, its extensions, and generated C library and allocator code as inputs; the [FTS5 build](https://github.com/ncruces/go-sqlite3-wasm/tree/v3.2.35304/fts5/build) uses the same SQLite release. SQLite states that its deliverable code is [dedicated to the public domain](https://www.sqlite.org/copyright.html). The translation's [README](https://github.com/ncruces/go-sqlite3-wasm/blob/v3.2.35304/README.md) states that the original licenses remain in effect for translated code.

The translation module selects `github.com/ncruces/wasm2go` v0.4.11 as its generator. Its `libc-gen` supplies the C and Go support code used by the build, including [Doug Lea's malloc](https://github.com/ncruces/wasm2go/blob/v0.4.11/libc-gen/c/malloc.c), which is included by the core build through `malloc_sbrk.c`. The following texts are reproduced from that version's [license](https://github.com/ncruces/wasm2go/blob/v0.4.11/LICENSE) and allocator source.

#### wasm2go v0.4.11

MIT License

Copyright (c) 2026 Nuno Cruces

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

#### Doug Lea's malloc 2.8.6

Copyright 2023 Doug Lea

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

Known limits: The supporting-code inventory above is not a complete audit of source-file copyright notices, embedded assets or generated code, Go standard-library vendored code, native libraries, or container base images and OS packages. In particular, review Debian `libsqlite3-0` and the other installed packages in the actual server image. The exporter is outside this inventory. Check the actual release images, dependency versions, and upstream source for additional obligations. The Compos project license is specified in LICENSE.
