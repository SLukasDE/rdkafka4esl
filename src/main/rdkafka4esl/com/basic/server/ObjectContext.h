#ifndef RDKAFKA4ESL_COM_BASIC_SERVER_OBJECTCONTEXT_H_
#define RDKAFKA4ESL_COM_BASIC_SERVER_OBJECTCONTEXT_H_

#include <esl/object/Interface.h>
#include <esl/object/ObjectContext.h>

#include <string>
#include <map>
#include <memory>

namespace rdkafka4esl {
namespace com {
namespace basic {
namespace server {

class ObjectContext final : public esl::object::ObjectContext {
public:
	void addObject(const std::string& id, std::unique_ptr<esl::object::Interface::Object> object) override;

protected:
	esl::object::Interface::Object* findRawObject(const std::string& id) override;
	const esl::object::Interface::Object* findRawObject(const std::string& id) const override;

private:
	std::map<std::string, std::unique_ptr<esl::object::Interface::Object>> objects;
};

} /* namespace server */
} /* namespace basic */
} /* namespace com */
} /* namespace rdkafka4esl */

#endif /* RDKAFKA4ESL_COM_BASIC_SERVER_OBJECTCONTEXT_H_ */
