#include <rdkafka4esl/messaging/MessageContext.h>

#include <esl/messaging/Interface.h>

namespace rdkafka4esl {
namespace messaging {

MessageContext::MessageContext(esl::messaging::Message& aMessage, const Client& aClient)
: message(aMessage),
  client(aClient)
{ }

esl::messaging::Message& MessageContext::getMessage() const {
	return message;
}

esl::object::Interface::Object* MessageContext::findObject(const std::string& id) const {
	esl::messaging::Interface::Client::ObjectFactory objectFactory = client.getObjectFactory(id);
	if(objectFactory) {
		return objectFactory(*this);
	}
	return nullptr;
}

} /* namespace messaging */
} /* namespace rdkafka4esl */
