// Dandelion request/response wire format:
//   <varint version><varint metadata length><protobuf metadata>[payload]
// Decodes InvocationBody/RegistrationBody output with the generated protobuf
// types, and parses hand-built DandelionResponse frames (incl. malformed ones).

#include "test_util.hpp"

#include "plume/dandelion/api.hpp"
#include "plume/dandelion/composition.hpp"

#include "dandelion.pb.h"

#include <string>

using namespace plume;
using namespace plume::dandelion;
namespace dfe = ::dandelion_frontend;

namespace {

BinaryData Bytes(const std::string &s) {
    return BinaryData(s.begin(), s.end());
}

void AppendVarint(BinaryData &out, uint64_t v) {
    while (v >= 0x80) {
        out.push_back(static_cast<uint8_t>(v | 0x80));
        v >>= 7;
    }
    out.push_back(static_cast<uint8_t>(v));
}

uint64_t ReadVarint(const BinaryData &in, size_t &pos) {
    uint64_t v = 0;
    for (unsigned shift = 0;; shift += 7) {
        uint8_t b = in.at(pos++);
        v |= static_cast<uint64_t>(b & 0x7f) << shift;
        if (!(b & 0x80)) return v;
    }
}

// Splits a frame into (metadata bytes, payload bytes), checking the version.
std::pair<std::string, BinaryData> Unframe(const BinaryData &body) {
    size_t pos = 0;
    CHECK(ReadVarint(body, pos) == kWireVersion);
    uint64_t meta_len = ReadVarint(body, pos);
    CHECK(pos + meta_len <= body.size());
    std::string meta(body.begin() + pos, body.begin() + pos + meta_len);
    return {meta, BinaryData(body.begin() + pos + meta_len, body.end())};
}

BinaryData Frame(const dfe::DandelionResponse &meta, const BinaryData &payload, uint64_t version = kWireVersion) {
    BinaryData out;
    AppendVarint(out, version);
    std::string m = meta.SerializeAsString();
    AppendVarint(out, m.size());
    out.insert(out.end(), m.begin(), m.end());
    out.insert(out.end(), payload.begin(), payload.end());
    return out;
}

DandelionComposition MakeComposition() {
    DandelionComposition comp;
    comp.name = "q1";
    comp.dsl = "composition q1 (a, b) => (c) { ... }";
    comp.in_sets = {
        {{7, "a0", Bytes("hello")}, {8, "a1", Bytes("")}},
        {},
        {{0, "b0", Bytes(std::string(200, 'x'))}},
    };
    return comp;
}

} // namespace

TEST_CASE("invocation body: inline composition, metadata + concatenated payload") {
    auto comp = MakeComposition();
    auto body = InvocationBody(comp, /*is_registered=*/false).unwrap();
    auto [meta_bytes, payload] = Unframe(body);

    dfe::DandelionRequest req;
    CHECK(req.ParseFromString(meta_bytes));
    CHECK(req.target_case() == dfe::DandelionRequest::kComposition);
    CHECK(req.composition() == comp.dsl);
    CHECK(req.sets_size() == 3);
    CHECK(req.sets(0).items_size() == 2);
    CHECK(req.sets(0).items(0).identifier() == "a0");
    CHECK(req.sets(0).items(0).key() == 7);
    CHECK(req.sets(0).items(0).payload_length() == 5);
    CHECK(req.sets(0).items(1).payload_length() == 0);
    CHECK(req.sets(1).items_size() == 0);
    CHECK(req.sets(2).items(0).payload_length() == 200);
    CHECK(payload == Bytes("hello" + std::string(200, 'x')));
}

TEST_CASE("invocation body: registered composition is referenced by name") {
    auto body = InvocationBody(MakeComposition(), /*is_registered=*/true).unwrap();
    dfe::DandelionRequest req;
    CHECK(req.ParseFromString(Unframe(body).first));
    CHECK(req.target_case() == dfe::DandelionRequest::kName);
    CHECK(req.name() == "q1");
}

TEST_CASE("invocation body: key must fit 32 bits") {
    auto comp = MakeComposition();
    comp.in_sets[0][0].key = uint64_t(1) << 32;
    CHECK(InvocationBody(comp).is_error());
}

TEST_CASE("registration body") {
    auto comp = MakeComposition();
    auto [meta_bytes, payload] = Unframe(RegistrationBody(comp).unwrap());
    dfe::RegisterComposition reg;
    CHECK(reg.ParseFromString(meta_bytes));
    CHECK(reg.composition() == comp.dsl);
    CHECK(payload.empty());
}

TEST_CASE("response: sets, items, payload slicing and trace") {
    dfe::DandelionResponse meta;
    auto *s0 = meta.add_sets();
    auto *i0 = s0->add_items();
    i0->set_identifier("out0");
    i0->set_key(3);
    i0->set_payload_length(3);
    auto *i1 = s0->add_items();
    i1->set_identifier("out1");
    i1->set_payload_length(4);
    meta.add_sets(); // empty set
    meta.set_trace("{\"ts\":[]}");

    std::string trace;
    auto sets = ParseResponseBody(Frame(meta, Bytes("abcdefg")), &trace).unwrap();
    CHECK(sets.size() == 2);
    CHECK(sets[0].size() == 2);
    CHECK(sets[0][0].identifier == "out0");
    CHECK(sets[0][0].key == 3);
    CHECK(sets[0][0].data == Bytes("abc"));
    CHECK(sets[0][1].data == Bytes("defg"));
    CHECK(sets[1].empty());
    CHECK(trace == "{\"ts\":[]}");
}

TEST_CASE("response: missing trace yields empty string") {
    dfe::DandelionResponse meta;
    meta.add_sets();
    std::string trace = "stale";
    CHECK(ParseResponseBody(Frame(meta, {}), &trace).is_ok());
    CHECK(trace.empty());
}

TEST_CASE("response: malformed frames are rejected") {
    dfe::DandelionResponse meta;
    meta.add_sets()->add_items()->set_payload_length(4);

    CHECK(ParseResponseBody(Frame(meta, Bytes("abc"))).is_error());               // payload too short
    CHECK(ParseResponseBody(Frame(meta, Bytes("abcde"))).is_error());             // trailing bytes
    CHECK(ParseResponseBody(Frame(meta, Bytes("abcd"), /*version=*/2)).is_error()); // wrong version
    CHECK(ParseResponseBody(BinaryData{}).is_error());                            // empty
    CHECK(ParseResponseBody(BinaryData{1, 50, 0}).is_error());                    // metadata overruns body
    CHECK(ParseResponseBody(BinaryData{1, 2, 0xff, 0xff}).is_error());            // garbage metadata
}

int main() {
    return plume_test::RunAll();
}
