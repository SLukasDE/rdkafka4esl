#include <rdkafka4esl/com/basic/server/ObjectContext.h>

namespace rdkafka4esl {
namespace com {
namespace basic {
namespace server {

void ObjectContext::addObject(const std::string& id, std::unique_ptr<esl::object::Interface::Object> object) {
	objects[id] = std::move(object);
}

esl::object::Interface::Object* ObjectContext::findRawObject(const std::string& id) const {
	auto iter = objects.find(id);
	return iter == std::end(objects) ? nullptr : iter->second.get();
}

} /* namespace server */
} /* namespace basic */
} /* namespace com */
} /* namespace rdkafka4esl */
