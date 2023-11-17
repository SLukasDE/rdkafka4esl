#ifndef ESL_COM_BASIC_CLIENT_KAFKACONNECTIONFACTORY_H_
#define ESL_COM_BASIC_CLIENT_KAFKACONNECTIONFACTORY_H_

#include <esl/com/basic/client/Connection.h>
#include <esl/com/basic/client/ConnectionFactory.h>
#include <esl/object/Context.h>
#include <esl/object/InitializeContext.h>
#include <esl/object/Object.h>

#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

namespace esl {
inline namespace v1_6 {
namespace com {
namespace basic {
namespace client {

class KafkaConnectionFactory : public ConnectionFactory, public object::InitializeContext {
public:
	struct Settings {
		Settings(const std::vector<std::pair<std::string, std::string>>& settings);

		std::string brokerId;
		std::string topicName;
		std::string key;
		std::int32_t partition = ((int32_t)-1);
		std::vector<std::pair<std::string, std::string>> topicParameters;
	};

	KafkaConnectionFactory(const Settings& settings);

	static std::unique_ptr<ConnectionFactory> create(const std::vector<std::pair<std::string, std::string>>& settings);

	std::unique_ptr<Connection> createConnection() const override;

	void initializeContext(object::Context& context) override;

private:
	KafkaConnectionFactory(std::tuple<std::unique_ptr<Object>, ConnectionFactory&, object::InitializeContext&> object);

	std::tuple<std::unique_ptr<Object>, ConnectionFactory&, object::InitializeContext&> object;
};

} /* namespace client */
} /* namespace basic */
} /* namespace com */
} /* inline namespace v1_6 */
} /* namespace esl */

#endif /* ESL_COM_BASIC_CLIENT_KAFKACONNECTIONFACTORY_H_ */
