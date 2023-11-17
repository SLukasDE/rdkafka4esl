#ifndef RDKAFKA4ESL_COM_BASIC_SERVER_OBJECTCONTEXT_H_
#define RDKAFKA4ESL_COM_BASIC_SERVER_OBJECTCONTEXT_H_

#include <esl/object/Object.h>
#include <esl/object/Context.h>

#include <set>
#include <string>
#include <map>
#include <memory>

namespace rdkafka4esl {
namespace com {
namespace basic {
namespace server {

class ObjectContext final : public esl::object::Context {
public:
	void addObject(const std::string& id, std::unique_ptr<esl::object::Object> object) override;
	std::set<std::string> getObjectIds() const override;

protected:
	esl::object::Object* findRawObject(const std::string& id) override;
	const esl::object::Object* findRawObject(const std::string& id) const override;

private:
	std::map<std::string, std::unique_ptr<esl::object::Object>> objects;
};

} /* namespace server */
} /* namespace basic */
} /* namespace com */
} /* namespace rdkafka4esl */

#endif /* RDKAFKA4ESL_COM_BASIC_SERVER_OBJECTCONTEXT_H_ */
