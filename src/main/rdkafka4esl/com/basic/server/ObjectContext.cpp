#include <rdkafka4esl/com/basic/server/ObjectContext.h>

namespace rdkafka4esl {
namespace com {
namespace basic {
namespace server {

std::set<std::string> ObjectContext::getObjectIds() const {
	std::set<std::string> rv;

	for(const auto& object : objects) {
		rv.insert(object.first);
	}

	return rv;
}

esl::object::Object* ObjectContext::findRawObject(const std::string& id) {
	auto iter = objects.find(id);
	return iter == std::end(objects) ? nullptr : iter->second.get();
}

const esl::object::Object* ObjectContext::findRawObject(const std::string& id) const {
	auto iter = objects.find(id);
	return iter == std::end(objects) ? nullptr : iter->second.get();
}

void ObjectContext::addRawObject(const std::string& id, std::unique_ptr<esl::object::Object> object) {
	objects[id] = std::move(object);
}

} /* namespace server */
} /* namespace basic */
} /* namespace com */
} /* namespace rdkafka4esl */
