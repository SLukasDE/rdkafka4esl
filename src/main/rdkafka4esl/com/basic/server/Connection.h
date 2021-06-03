#ifndef RDKAFKA4ESL_COM_BASIC_SERVER_CONNECTION_H_
#define RDKAFKA4ESL_COM_BASIC_SERVER_CONNECTION_H_

#include <esl/com/basic/server/Connection.h>
#include <esl/io/Output.h>

#include <vector>
#include <string>
#include <utility>

namespace rdkafka4esl {
namespace com {
namespace basic {
namespace server {

class Connection : public esl::com::basic::server::Connection {
public:
	bool send(esl::io::Output output, std::vector<std::pair<std::string, std::string>> parameters) override;
};

} /* namespace server */
} /* namespace basic */
} /* namespace com */
} /* namespace rdkafka4esl */

#endif /* RDKAFKA4ESL_COM_BASIC_SERVER_CONNECTION_H_ */
