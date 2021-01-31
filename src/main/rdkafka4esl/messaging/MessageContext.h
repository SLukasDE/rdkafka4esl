#ifndef RDKAFKA4ESL_MESSAGING_MESSAGECONTEXT_H_
#define RDKAFKA4ESL_MESSAGING_MESSAGECONTEXT_H_

#include <rdkafka4esl/messaging/Client.h>

#include <esl/messaging/MessageContext.h>
#include <esl/messaging/Message.h>
#include <esl/object/Interface.h>

namespace rdkafka4esl {
namespace messaging {

class MessageContext : public esl::messaging::MessageContext {
public:
	MessageContext(esl::messaging::Message& message, const Client& client);

	esl::messaging::Message& getMessage() const override;

	esl::object::Interface::Object* findObject(const std::string& id) const override;

private:
	esl::messaging::Message& message;
	const Client& client;
};

} /* namespace messaging */
} /* namespace rdkafka4esl */

#endif /* RDKAFKA4ESL_MESSAGING_MESSAGECONTEXT_H_ */
