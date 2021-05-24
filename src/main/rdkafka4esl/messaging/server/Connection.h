#ifndef RDKAFKA4ESL_MESSAGING_SERVER_CONNECTION_H_
#define RDKAFKA4ESL_MESSAGING_SERVER_CONNECTION_H_

#include <esl/messaging/server/Connection.h>
#include <esl/io/Output.h>

#include <vector>
#include <string>
#include <utility>

namespace rdkafka4esl {
namespace messaging {
namespace server {

class Connection : public esl::messaging::server::Connection {
public:
	bool sendMessage(esl::io::Output output, std::vector<std::pair<std::string, std::string>> parameters) override;
};

} /* namespace server */
} /* namespace messaging */
} /* namespace rdkafka4esl */

#endif /* RDKAFKA4ESL_MESSAGING_SERVER_CONNECTION_H_ */
