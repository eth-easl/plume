#include "plume/expression/expression.hpp"

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"

namespace plume::expr {

bool ExprNode::Equals(const ExprNode &other) const {
    if (kind != other.kind) return false;
    if (!(return_type == other.return_type)) return false;
    if (ref_index != other.ref_index) return false;
    if (constant.IsNull() != other.constant.IsNull()) return false;
    if (!constant.IsNull() && !(constant == other.constant)) return false;
    if (func_name != other.func_name) return false;
    if (expr_type != other.expr_type) return false;
    if (try_cast != other.try_cast) return false;
    if (children.size() != other.children.size()) return false;
    for (size_t i = 0; i < children.size(); i++) {
        if (!children[i].Equals(other.children[i])) return false;
    }
    return true;
}

void ExprNode::Serialize(duckdb::Serializer &s) const {
    s.WriteProperty(100, "kind", static_cast<uint8_t>(kind));
    s.WriteProperty(101, "return_type", return_type);
    s.WriteProperty(102, "ref_index", ref_index);
    s.WriteProperty(103, "expr_type", expr_type);
    s.WriteProperty(104, "try_cast", try_cast);
    s.WriteProperty(105, "func_name", func_name);
    s.WriteProperty(106, "constant", constant);
    s.WriteList(107, "children", children.size(), [&](duckdb::Serializer::List &list, duckdb::idx_t i) {
        list.WriteElement(children[i]);
    });
}

ExprNode ExprNode::Deserialize(duckdb::Deserializer &d) {
    ExprNode n;
    n.kind = static_cast<ExprKind>(d.ReadProperty<uint8_t>(100, "kind"));
    n.return_type = d.ReadProperty<ColumnType>(101, "return_type");
    n.ref_index = d.ReadProperty<uint32_t>(102, "ref_index");
    n.expr_type = d.ReadProperty<uint8_t>(103, "expr_type");
    n.try_cast = d.ReadProperty<bool>(104, "try_cast");
    n.func_name = d.ReadProperty<std::string>(105, "func_name");
    n.constant = d.ReadProperty<duckdb::Value>(106, "constant");
    d.ReadList(107, "children", [&](duckdb::Deserializer::List &list, duckdb::idx_t /*i*/) {
        n.children.push_back(list.ReadElement<ExprNode>());
    });
    return n;
}

} // namespace plume::expr
