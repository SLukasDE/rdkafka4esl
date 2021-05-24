#include <rdkafka4esl/messaging/server/Connection.h>

namespace rdkafka4esl {
namespace messaging {
namespace server {

bool Connection::sendMessage(esl::io::Output output, std::vector<std::pair<std::string, std::string>> parameters) {
	return false;
}

} /* namespace server */
} /* namespace messaging */
} /* namespace rdkafka4esl */
