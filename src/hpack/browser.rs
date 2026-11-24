//! Header compression that reproduces the HPACK encoders of browsers: their
//! choice of representation for each field, their dynamic table use and their
//! string encoding, so that header blocks come out byte for byte as the
//! browser writes them.

use super::encoder::encode_int;
use super::{huffman, Header};

use bytes::{BufMut, Bytes, BytesMut};
use http::header::HeaderName;
use std::collections::VecDeque;

/// How the client compresses the header blocks it sends.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum HeaderCompression {
    /// This crate's encoder.
    #[default]
    Default,

    /// Firefox (`Http2Compressor`, netwerk/protocol/http/Http2Compression.cpp):
    /// every string Huffman-coded; `:path` as a literal without indexing, also
    /// when the table holds it; a literal names the last table entry with its
    /// name (static entries first, then the dynamic ones from the newest);
    /// fields larger than half the table not indexed; cookies split into
    /// crumbs at `"; "`, crumbs shorter than 20 bytes and `authorization`
    /// never indexed.
    Firefox,

    /// Chromium (quiche `HpackEncoder`): strings Huffman-coded only when that
    /// is shorter; every regular field and `:authority` indexed, the other
    /// pseudo-header fields not; a literal names the first static entry with
    /// its name, else the newest dynamic one; cookies split into crumbs at
    /// `;`.
    Chromium,

    /// Safari (CFNetwork, captured from Safari on macOS 14, 15, 26 and 27 and
    /// on iOS 17 to 27): strings Huffman-coded only when that is shorter;
    /// `:path` and `content-length` as literals without indexing, cookies as
    /// never-indexed crumbs split at `"; "`, every other field indexed
    /// (`:method CONNECT` and `:protocol` included); a literal names the first
    /// static entry with its name, else the newest dynamic one. Like Firefox,
    /// it announces every SETTINGS_HEADER_TABLE_SIZE it receives.
    Safari,
}

/// The browser encoders.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Style {
    Firefox,
    Chromium,
    Safari,
}

/// An HPACK encoder that follows a browser's rules (see
/// [`HeaderCompression`]).
#[derive(Debug)]
pub(crate) struct BrowserEncoder {
    style: Style,
    /// The dynamic table, newest entry first.
    table: VecDeque<(Bytes, Bytes)>,
    /// Size of the dynamic table (RFC 7541 §4.1).
    size: usize,
    /// Maximum size of the dynamic table.
    max_size: usize,
    /// Dynamic table size updates the next header block starts with: the
    /// smallest size since the last block, then the current one.
    size_update: Option<(usize, usize)>,
}

/// Field representations (RFC 7541 §6).
const INDEXED: (u8, usize) = (0x80, 7);
const INCREMENTAL: (u8, usize) = (0x40, 6);
const WITHOUT_INDEXING: (u8, usize) = (0x00, 4);
const NEVER_INDEXED: (u8, usize) = (0x10, 4);

impl BrowserEncoder {
    /// An encoder for `compression`, `None` for this crate's own.
    pub(crate) fn new(compression: HeaderCompression, max_size: usize) -> Option<BrowserEncoder> {
        let style = match compression {
            HeaderCompression::Default => return None,
            HeaderCompression::Firefox => Style::Firefox,
            HeaderCompression::Chromium => Style::Chromium,
            HeaderCompression::Safari => Style::Safari,
        };
        Some(BrowserEncoder {
            style,
            table: VecDeque::new(),
            size: 0,
            max_size,
            size_update: None,
        })
    }

    /// The peer changed SETTINGS_HEADER_TABLE_SIZE.
    pub(crate) fn update_max_size(&mut self, val: usize) {
        let smallest = match self.style {
            // Firefox announces every setting it receives, also an unchanged
            // one (`Http2Compressor::SetMaxBufferSize`); so does Safari (a
            // size update of 4096 after a server's 4096).
            Style::Firefox | Style::Safari => self
                .size_update
                .map_or(val, |(smallest, _)| smallest.min(val)),
            // Chromium ignores an unchanged setting and remembers only
            // settings below the previous one
            // (`HpackEncoder::ApplyHeaderTableSizeSetting`).
            Style::Chromium => {
                if val == self.max_size {
                    return;
                }
                let smallest = self
                    .size_update
                    .map_or(usize::MAX, |(smallest, _)| smallest);
                if val < self.max_size {
                    smallest.min(val)
                } else {
                    smallest
                }
            }
        };
        self.size_update = Some((smallest, val));
        self.max_size = val;
        self.evict_to(val);
    }

    /// Encode a header block.
    pub(crate) fn encode<I>(&mut self, headers: I, dst: &mut BytesMut)
    where
        I: IntoIterator<Item = Header<Option<HeaderName>>>,
    {
        if let Some((smallest, size)) = self.size_update.take() {
            if smallest < size {
                encode_int(smallest, 5, 0x20, dst);
            }
            encode_int(size, 5, 0x20, dst);
        }

        let mut last_name: Option<HeaderName> = None;
        for header in headers {
            let (name, value): (Bytes, Bytes) = match header {
                Header::Field { name, value } => {
                    if let Some(name) = name {
                        last_name = Some(name);
                    }
                    let name = last_name
                        .as_ref()
                        .expect("encoding header without a name, but no previous name to reuse");
                    (
                        Bytes::copy_from_slice(name.as_str().as_bytes()),
                        Bytes::copy_from_slice(value.as_bytes()),
                    )
                }
                Header::Authority(v) => (Bytes::from_static(b":authority"), v.into_inner()),
                Header::Method(v) => (
                    Bytes::from_static(b":method"),
                    Bytes::copy_from_slice(v.as_str().as_bytes()),
                ),
                Header::Scheme(v) => (Bytes::from_static(b":scheme"), v.into_inner()),
                Header::Path(v) => (Bytes::from_static(b":path"), v.into_inner()),
                Header::Protocol(v) => (
                    Bytes::from_static(b":protocol"),
                    Bytes::copy_from_slice(v.as_str().as_bytes()),
                ),
                Header::Status(v) => (
                    Bytes::from_static(b":status"),
                    Bytes::copy_from_slice(v.as_str().as_bytes()),
                ),
            };
            match self.style {
                Style::Firefox => self.encode_firefox(name, value, dst),
                Style::Chromium => self.encode_chromium(name, value, dst),
                Style::Safari => self.encode_safari(name, value, dst),
            }
        }
    }

    /// Safari: cookie crumbs never indexed, `:path` and `content-length` not
    /// indexed, everything else indexed.
    fn encode_safari(&mut self, name: Bytes, value: Bytes, dst: &mut BytesMut) {
        if &name[..] == b"cookie" {
            for crumb in split_crumbs(&value, b"; ", 2) {
                let name_ref = self.first_name_ref(&name);
                self.literal(NEVER_INDEXED, name_ref, &name, crumb, dst);
            }
            return;
        }
        if self.encode_if_full_match(&name, &value, dst) {
            return;
        }
        let index = &name[..] != b":path" && &name[..] != b"content-length";
        self.encode_indexed_or_literal(&name, &value, index, dst);
    }

    /// Encodes `name`/`value` as `INDEXED` if it's already a full match in
    /// the table. Returns whether it was.
    fn encode_if_full_match(&mut self, name: &[u8], value: &[u8], dst: &mut BytesMut) -> bool {
        let Some((index, _)) = self.entries().find(|(_, (n, v))| *n == name && *v == value) else {
            return false;
        };
        encode_int(index, INDEXED.1, INDEXED.0, dst);
        true
    }

    /// Encodes a header that isn't a full match: `WITHOUT_INDEXING` when
    /// `index` is false, else `INCREMENTAL` followed by inserting it into
    /// the dynamic table, evicting older entries first. An entry that
    /// doesn't fit even in an empty table empties it instead
    /// (`HpackHeaderTable::TryAddEntry`).
    fn encode_indexed_or_literal(
        &mut self,
        name: &[u8],
        value: &[u8],
        index: bool,
        dst: &mut BytesMut,
    ) {
        let name_ref = self.first_name_ref(name);
        if !index {
            self.literal(WITHOUT_INDEXING, name_ref, name, value, dst);
            return;
        }
        self.literal(INCREMENTAL, name_ref, name, value, dst);
        let size = entry_size(name, value);
        if size > self.max_size {
            self.evict_to(0);
        } else {
            self.evict_to(self.max_size - size);
            self.insert(name, value, size);
        }
    }

    /// The index of the first static entry named `name`, else of the newest
    /// dynamic one; 0 for none.
    fn first_name_ref(&self, name: &[u8]) -> usize {
        self.entries()
            .find(|(_, (n, _))| *n == name)
            .map_or(0, |(index, _)| index)
    }

    /// `Http2Compressor::EncodeHeaderBlock`: `:path` is never indexed,
    /// cookies go as crumbs, short crumbs and `authorization` never indexed.
    fn encode_firefox(&mut self, name: Bytes, value: Bytes, dst: &mut BytesMut) {
        match &name[..] {
            b":path" => self.process_firefox(&name, &value, true, false, dst),
            b"cookie" => {
                for crumb in split_crumbs(&value, b"; ", 2) {
                    self.process_firefox(&name, crumb, false, crumb.len() < 20, dst);
                }
            }
            b"authorization" => self.process_firefox(&name, &value, false, true, dst),
            _ => self.process_firefox(&name, &value, false, false, dst),
        }
    }

    /// `Http2Compressor::ProcessHeader`.
    fn process_firefox(
        &mut self,
        name: &[u8],
        value: &[u8],
        no_local_index: bool,
        never_index: bool,
        dst: &mut BytesMut,
    ) {
        // The last entry with the name, up to the first full match.
        let mut name_ref = 0;
        let mut matched = None;
        for (index, (n, v)) in self.entries() {
            if n == name {
                name_ref = index;
                if v == value {
                    matched = Some(index);
                    break;
                }
            }
        }

        if matched.is_none() || no_local_index || never_index {
            if never_index {
                self.literal(NEVER_INDEXED, name_ref, name, value, dst);
                return;
            }
            let size = entry_size(name, value);
            if no_local_index || size > self.max_size / 2 || self.max_size < 128 {
                self.literal(WITHOUT_INDEXING, name_ref, name, value, dst);
                return;
            }
            // Room first; the name reference was taken before, as a decoder
            // resolves it before inserting.
            self.evict_to(self.max_size.saturating_sub(size));
            self.literal(INCREMENTAL, name_ref, name, value, dst);
            self.insert(name, value, size);
            return;
        }

        // The early return above covers `matched.is_none()`.
        let Some(matched) = matched else {
            unreachable!()
        };
        encode_int(matched, INDEXED.1, INDEXED.0, dst);
    }

    /// `HpackEncoder::EncodeRepresentations` with the default indexing
    /// policy; cookies as crumbs (`CookieToCrumbs`).
    fn encode_chromium(&mut self, name: Bytes, value: Bytes, dst: &mut BytesMut) {
        if &name[..] == b"cookie" {
            let trimmed = trim(&value);
            for crumb in split_crumbs(trimmed, b";", 1) {
                let crumb = crumb.strip_prefix(b" ").unwrap_or(crumb);
                self.process_chromium(&name, crumb, dst);
            }
        } else {
            self.process_chromium(&name, &value, dst);
        }
    }

    fn process_chromium(&mut self, name: &[u8], value: &[u8], dst: &mut BytesMut) {
        if self.encode_if_full_match(name, value, dst) {
            return;
        }
        // Every regular field and :authority (`DefaultPolicy`).
        let index = !name.starts_with(b":") || name == b":authority";
        self.encode_indexed_or_literal(name, value, index, dst);
    }

    /// The static table, then the dynamic table from the newest entry, with
    /// their HPACK indices.
    fn entries(&self) -> impl Iterator<Item = (usize, (&[u8], &[u8]))> {
        STATIC_TABLE
            .iter()
            .map(|(n, v)| (n.as_bytes(), v.as_bytes()))
            .chain(self.table.iter().map(|(n, v)| (&n[..], &v[..])))
            .enumerate()
            .map(|(i, entry)| (i + 1, entry))
    }

    fn literal(
        &self,
        (first_byte, prefix): (u8, usize),
        name_ref: usize,
        name: &[u8],
        value: &[u8],
        dst: &mut BytesMut,
    ) {
        encode_int(name_ref, prefix, first_byte, dst);
        if name_ref == 0 {
            self.string(name, dst);
        }
        self.string(value, dst);
    }

    /// A string literal: always Huffman-coded by Firefox
    /// (`Http2Compressor::HuffmanAppend`), only when shorter by Chromium
    /// (`HpackEncoder::EmitString`).
    fn string(&self, value: &[u8], dst: &mut BytesMut) {
        let mut coded = BytesMut::with_capacity(value.len());
        huffman::encode(value, &mut coded);
        if self.style == Style::Firefox || coded.len() < value.len() {
            encode_int(coded.len(), 7, 0x80, dst);
            dst.put_slice(&coded);
        } else {
            encode_int(value.len(), 7, 0, dst);
            dst.put_slice(value);
        }
    }

    fn insert(&mut self, name: &[u8], value: &[u8], size: usize) {
        self.table
            .push_front((Bytes::copy_from_slice(name), Bytes::copy_from_slice(value)));
        self.size += size;
    }

    /// Evict the oldest entries until the table is at most `size` bytes.
    fn evict_to(&mut self, size: usize) {
        while self.size > size {
            match self.table.pop_back() {
                Some((n, v)) => self.size -= entry_size(&n, &v),
                None => break,
            }
        }
    }
}

/// Size of a table entry (RFC 7541 §4.1).
fn entry_size(name: &[u8], value: &[u8]) -> usize {
    32 + name.len() + value.len()
}

/// Leading and trailing spaces and tabs removed.
fn trim(value: &[u8]) -> &[u8] {
    let blank = |b: &u8| *b == b' ' || *b == b'\t';
    let start = value.iter().position(|b| !blank(b)).unwrap_or(value.len());
    let end = value
        .iter()
        .rposition(|b| !blank(b))
        .map_or(start, |i| i + 1);
    &value[start..end]
}

/// `value` split at every `separator` (whose first `skip` bytes are
/// dropped).
fn split_crumbs<'a>(
    value: &'a [u8],
    separator: &'static [u8],
    skip: usize,
) -> impl Iterator<Item = &'a [u8]> {
    let mut rest = Some(value);
    std::iter::from_fn(move || {
        let current = rest?;
        match current
            .windows(separator.len())
            .position(|window| window == separator)
        {
            Some(at) => {
                rest = Some(&current[at + skip..]);
                Some(&current[..at])
            }
            None => {
                rest = None;
                Some(current)
            }
        }
    })
}

/// The HPACK static table (RFC 7541 Appendix A).
const STATIC_TABLE: [(&str, &str); 61] = [
    (":authority", ""),
    (":method", "GET"),
    (":method", "POST"),
    (":path", "/"),
    (":path", "/index.html"),
    (":scheme", "http"),
    (":scheme", "https"),
    (":status", "200"),
    (":status", "204"),
    (":status", "206"),
    (":status", "304"),
    (":status", "400"),
    (":status", "404"),
    (":status", "500"),
    ("accept-charset", ""),
    ("accept-encoding", "gzip, deflate"),
    ("accept-language", ""),
    ("accept-ranges", ""),
    ("accept", ""),
    ("access-control-allow-origin", ""),
    ("age", ""),
    ("allow", ""),
    ("authorization", ""),
    ("cache-control", ""),
    ("content-disposition", ""),
    ("content-encoding", ""),
    ("content-language", ""),
    ("content-length", ""),
    ("content-location", ""),
    ("content-range", ""),
    ("content-type", ""),
    ("cookie", ""),
    ("date", ""),
    ("etag", ""),
    ("expect", ""),
    ("expires", ""),
    ("from", ""),
    ("host", ""),
    ("if-match", ""),
    ("if-modified-since", ""),
    ("if-none-match", ""),
    ("if-range", ""),
    ("if-unmodified-since", ""),
    ("last-modified", ""),
    ("link", ""),
    ("location", ""),
    ("max-forwards", ""),
    ("proxy-authenticate", ""),
    ("proxy-authorization", ""),
    ("range", ""),
    ("referer", ""),
    ("refresh", ""),
    ("retry-after", ""),
    ("server", ""),
    ("set-cookie", ""),
    ("strict-transport-security", ""),
    ("transfer-encoding", ""),
    ("user-agent", ""),
    ("vary", ""),
    ("via", ""),
    ("www-authenticate", ""),
];

#[cfg(test)]
mod test {
    use super::*;
    use crate::hpack::{BytesStr, Decoder};
    use http::header::HeaderValue;
    use http::Method;

    fn field(name: &str, value: &str) -> Header<Option<HeaderName>> {
        Header::Field {
            name: Some(HeaderName::from_bytes(name.as_bytes()).unwrap()),
            value: HeaderValue::from_str(value).unwrap(),
        }
    }

    fn request(path: &str, fields: &[(&str, &str)]) -> Vec<Header<Option<HeaderName>>> {
        let mut headers = vec![
            Header::Method(Method::GET),
            Header::Path(BytesStr::from(path)),
            Header::Authority(BytesStr::from("example.com")),
            Header::Scheme(BytesStr::from("https")),
        ];
        headers.extend(fields.iter().map(|(n, v)| field(n, v)));
        headers
    }

    fn encode(encoder: &mut BrowserEncoder, headers: Vec<Header<Option<HeaderName>>>) -> BytesMut {
        let mut dst = BytesMut::new();
        encoder.encode(headers, &mut dst);
        dst
    }

    /// The representation of each field: `idxN`, `incN`, `noidxN`,
    /// `neverN` with the name index (0 for a literal name) and `/H` or
    /// `/raw` for the value.
    fn representations(block: &[u8]) -> Vec<String> {
        let mut out = Vec::new();
        let mut i = 0;
        let int = |i: &mut usize, prefix: u32| -> usize {
            let mask = (1usize << prefix) - 1;
            let mut v = block[*i] as usize & mask;
            *i += 1;
            if v == mask {
                let mut m = 0;
                loop {
                    let b = block[*i] as usize;
                    *i += 1;
                    v += (b & 0x7f) << m;
                    m += 7;
                    if b & 0x80 == 0 {
                        break;
                    }
                }
            }
            v
        };
        let string = |i: &mut usize| -> &'static str {
            let huffman = block[*i] & 0x80 != 0;
            let len = int(i, 7);
            *i += len;
            if huffman {
                "H"
            } else {
                "raw"
            }
        };
        while i < block.len() {
            let b = block[i];
            let (kind, prefix) = if b & 0x80 != 0 {
                out.push(format!("idx{}", int(&mut i, 7)));
                continue;
            } else if b & 0x40 != 0 {
                ("inc", 6)
            } else if b & 0x20 != 0 {
                out.push(format!("size{}", int(&mut i, 5)));
                continue;
            } else if b & 0x10 != 0 {
                ("never", 4)
            } else {
                ("noidx", 4)
            };
            let index = int(&mut i, prefix);
            if index == 0 {
                string(&mut i);
            }
            out.push(format!("{kind}{index}/{}", string(&mut i)));
        }
        out
    }

    fn decode(decoder: &mut Decoder, block: &BytesMut) -> Vec<(String, String)> {
        let mut out = Vec::new();
        let mut buf = block.clone();
        let mut src = std::io::Cursor::new(&mut buf);
        decoder
            .decode(&mut src, |header| {
                let name = String::from_utf8(header.name().as_slice().to_vec()).unwrap();
                let value = String::from_utf8(header.value_slice().to_vec()).unwrap();
                out.push((name, value));
                std::ops::ControlFlow::Continue(())
            })
            .unwrap();
        out
    }

    #[test]
    fn firefox_matches_the_captured_navigations() {
        // Captured from Firefox 156 (two navigations on one connection).
        let mut encoder = BrowserEncoder::new(HeaderCompression::Firefox, 4096).unwrap();
        let fields = [
            ("user-agent", "Mozilla/5.0"),
            ("accept", "text/html"),
            ("accept-language", "en-US,en;q=0.9"),
            ("accept-encoding", "gzip, deflate, br, zstd"),
            ("upgrade-insecure-requests", "1"),
            ("te", "trailers"),
        ];
        let first = encode(&mut encoder, request("/", &fields));
        assert_eq!(
            representations(&first),
            [
                "idx2", "noidx4/H", "inc1/H", "idx7", "inc58/H", "inc19/H", "inc17/H", "inc16/H",
                "inc0/H", "inc0/H"
            ]
        );
        let mut later = fields.to_vec();
        later.insert(4, ("cookie", "__cf_bm=0123456789abcdefghij"));
        let second = encode(&mut encoder, request("/cdn-cgi/trace?nav", &later));
        let reps = representations(&second);
        // Other paths name the last :path entry; the cookie is indexed.
        assert_eq!(&reps[..4], ["idx2", "noidx5/H", "idx68", "idx7"]);
        assert_eq!(reps[8], "inc32/H");
    }

    #[test]
    fn firefox_crumbs_and_never_indexed_fields() {
        let mut encoder = BrowserEncoder::new(HeaderCompression::Firefox, 4096).unwrap();
        let block = encode(
            &mut encoder,
            vec![
                field("cookie", "a=1; long_cookie_name=0123456789"),
                field("authorization", "Basic abc"),
                field("cookie", "a=1"),
            ],
        );
        // The last cookie names the newest cookie entry, which comes after
        // the static one.
        assert_eq!(
            representations(&block),
            ["never32/H", "inc32/H", "never23/H", "never62/H"]
        );
        let mut decoder = Decoder::new(4096);
        let decoded = decode(&mut decoder, &block);
        assert_eq!(decoded[0], ("cookie".into(), "a=1".into()));
        assert_eq!(
            decoded[1],
            ("cookie".into(), "long_cookie_name=0123456789".into())
        );
    }

    #[test]
    fn firefox_names_the_last_entry_with_the_name() {
        let mut encoder = BrowserEncoder::new(HeaderCompression::Firefox, 4096).unwrap();
        encode(&mut encoder, vec![field("x-a", "1")]);
        encode(&mut encoder, vec![field("x-a", "2")]);
        // Two dynamic entries named x-a: 62 (newest) and 63 (oldest).
        let block = encode(&mut encoder, vec![field("x-a", "3")]);
        assert_eq!(representations(&block), ["inc63/H"]);
        // Fields larger than half the table stay out of it.
        let big = "v".repeat(2100);
        let block = encode(&mut encoder, vec![field("x-big", &big)]);
        assert_eq!(representations(&block), ["noidx0/H"]);
    }

    #[test]
    fn chromium_matches_the_captured_navigations() {
        // Captured from Chrome 153.
        let mut encoder = BrowserEncoder::new(HeaderCompression::Chromium, 4096).unwrap();
        let fields = [
            ("sec-ch-ua-mobile", "?0"),
            ("upgrade-insecure-requests", "1"),
            ("user-agent", "Mozilla/5.0"),
            ("accept", "*/*"),
            ("sec-fetch-user", "?1"),
            ("accept-encoding", "gzip, deflate, br, zstd"),
        ];
        let first = encode(&mut encoder, request("/", &fields));
        assert_eq!(
            representations(&first),
            [
                "idx2",
                "idx4",
                "inc1/H",
                "idx7",
                "inc0/raw",
                "inc0/raw",
                "inc58/H",
                "inc19/raw",
                "inc0/raw",
                "inc16/H"
            ]
        );
        let mut later = fields.to_vec();
        later.push(("cookie", "a=1; b=2"));
        let second = encode(&mut encoder, request("/cdn-cgi/trace?nav", &later));
        let reps = representations(&second);
        assert_eq!(&reps[..4], ["idx2", "noidx4/H", "idx68", "idx7"]);
        // Crumbs at ';', each indexed.
        assert_eq!(reps.len(), 12);
        assert!(reps[10].starts_with("inc32/") && reps[11].starts_with("inc32/"));
        let mut decoder = Decoder::new(4096);
        decode(&mut decoder, &first);
        let decoded = decode(&mut decoder, &second);
        assert_eq!(decoded[10], ("cookie".into(), "a=1".into()));
        assert_eq!(decoded[11], ("cookie".into(), "b=2".into()));
    }

    #[test]
    fn safari_matches_the_captured_requests() {
        // Captured from Safari 27.0 on macOS 27.0: a navigation, a fetch()
        // GET and a fetch() POST on one connection, after the server's
        // SETTINGS_HEADER_TABLE_SIZE of 4096.
        const UA: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 \
                          (KHTML, like Gecko) Version/27.0 Safari/605.1.15";
        let request = |method: Method, path: &str, fields: &[(&str, &str)]| {
            let mut headers = vec![
                Header::Method(method),
                Header::Scheme(BytesStr::from("https")),
                Header::Authority(BytesStr::from("capture.example.test")),
                Header::Path(BytesStr::from(path)),
            ];
            headers.extend(fields.iter().map(|(n, v)| field(n, v)));
            headers
        };
        let referer = "https://capture.example.test/start?auto=1";
        let mut encoder = BrowserEncoder::new(HeaderCompression::Safari, 4096).unwrap();
        encoder.update_max_size(4096);
        let navigation = encode(
            &mut encoder,
            request(
                Method::GET,
                "/start?auto=1",
                &[
                    ("sec-fetch-dest", "document"),
                    ("user-agent", UA),
                    (
                        "accept",
                        "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    ),
                    ("sec-fetch-site", "none"),
                    ("sec-fetch-mode", "navigate"),
                    ("accept-language", "en-US,en;q=0.9"),
                    ("priority", "u=0, i"),
                    ("accept-encoding", "gzip, deflate, br, zstd"),
                ],
            ),
        );
        assert_eq!(
            representations(&navigation),
            [
                "size4096", "idx2", "idx7", "inc1/H", "noidx4/H", "inc0/H", "inc58/H", "inc19/H",
                "inc0/H", "inc0/H", "inc17/H", "inc0/H", "inc16/H"
            ]
        );
        let fetch = encode(
            &mut encoder,
            request(
                Method::GET,
                "/api/data?x=1",
                &[
                    ("sec-fetch-dest", "empty"),
                    ("user-agent", UA),
                    ("accept", "*/*"),
                    ("referer", referer),
                    ("sec-fetch-site", "same-origin"),
                    ("sec-fetch-mode", "cors"),
                    ("accept-language", "en-US,en;q=0.9"),
                    ("priority", "u=3, i"),
                    ("accept-encoding", "gzip, deflate, br, zstd"),
                    ("cookie", "kc=1; kc2=2"),
                ],
            ),
        );
        assert_eq!(
            representations(&fetch),
            [
                "idx2",
                "idx7",
                "idx70",
                "noidx4/H",
                "inc69/H",
                "idx69",
                "inc19/raw",
                "inc51/H",
                "inc69/H",
                "inc69/H",
                "idx69",
                "inc68/H",
                "idx68",
                "never32/H",
                "never32/H"
            ]
        );
        let post = encode(
            &mut encoder,
            request(
                Method::POST,
                "/api/post",
                &[
                    ("accept", "*/*"),
                    ("content-type", "application/json"),
                    ("origin", "https://capture.example.test"),
                    ("sec-fetch-site", "same-origin"),
                    ("sec-fetch-mode", "cors"),
                    ("user-agent", UA),
                    ("referer", referer),
                    ("sec-fetch-dest", "empty"),
                    ("content-length", "7"),
                    ("accept-language", "en-US,en;q=0.9"),
                    ("priority", "u=3, i"),
                    ("accept-encoding", "gzip, deflate, br, zstd"),
                    ("cookie", "kc=1; kc2=2"),
                ],
            ),
        );
        assert_eq!(
            representations(&post),
            [
                "idx3",
                "idx7",
                "idx76",
                "noidx4/H",
                "idx66",
                "inc31/H",
                "inc0/H",
                "idx66",
                "idx65",
                "idx76",
                "idx67",
                "idx69",
                "noidx28/raw",
                "idx72",
                "idx64",
                "idx70",
                "never32/H",
                "never32/H"
            ]
        );
        let mut decoder = Decoder::new(4096);
        decode(&mut decoder, &navigation);
        let decoded = decode(&mut decoder, &fetch);
        assert_eq!(decoded[13], ("cookie".into(), "kc=1".into()));
        assert_eq!(decoded[14], ("cookie".into(), "kc2=2".into()));
    }

    #[test]
    fn safari_indexes_connect_and_protocol() {
        // Captured from Safari 27.0: the extended CONNECT of a WebSocket.
        let mut encoder = BrowserEncoder::new(HeaderCompression::Safari, 4096).unwrap();
        let block = encode(
            &mut encoder,
            vec![
                Header::Method(Method::CONNECT),
                Header::Scheme(BytesStr::from("https")),
                Header::Authority(BytesStr::from("ws.example.test")),
                Header::Path(BytesStr::from("/ws?id=c-end&close=client&srvend=1")),
                Header::Protocol(crate::ext::Protocol::from_static("websocket")),
            ],
        );
        assert_eq!(
            representations(&block),
            ["inc2/raw", "idx7", "inc1/H", "noidx4/H", "inc0/H"]
        );
    }

    #[test]
    fn table_size_updates() {
        let mut firefox = BrowserEncoder::new(HeaderCompression::Firefox, 4096).unwrap();
        firefox.update_max_size(4096);
        assert_eq!(representations(&encode(&mut firefox, vec![])), ["size4096"]);
        firefox.update_max_size(100);
        firefox.update_max_size(2000);
        assert_eq!(
            representations(&encode(&mut firefox, vec![])),
            ["size100", "size2000"]
        );

        let mut chromium = BrowserEncoder::new(HeaderCompression::Chromium, 4096).unwrap();
        chromium.update_max_size(4096);
        assert!(encode(&mut chromium, vec![]).is_empty());
        chromium.update_max_size(8192);
        assert_eq!(
            representations(&encode(&mut chromium, vec![])),
            ["size8192"]
        );

        let mut safari = BrowserEncoder::new(HeaderCompression::Safari, 4096).unwrap();
        assert!(encode(&mut safari, vec![]).is_empty());
        safari.update_max_size(4096);
        assert_eq!(representations(&encode(&mut safari, vec![])), ["size4096"]);
    }

    #[test]
    fn all_decode_to_the_input() {
        for compression in [
            HeaderCompression::Firefox,
            HeaderCompression::Chromium,
            HeaderCompression::Safari,
        ] {
            let mut encoder = BrowserEncoder::new(compression, 256).unwrap();
            let mut decoder = Decoder::new(256);
            for round in 0..5 {
                let value = format!("value-{round}-{}", "x".repeat(round * 20));
                let fields = [
                    ("x-one", value.as_str()),
                    ("x-two", "same"),
                    ("accept", "*/*"),
                ];
                let block = encode(&mut encoder, request("/", &fields));
                let decoded = decode(&mut decoder, &block);
                assert_eq!(
                    decoded[4],
                    ("x-one".into(), value.clone()),
                    "{compression:?}"
                );
                assert_eq!(decoded[6], ("accept".into(), "*/*".into()));
            }
        }
    }
}
