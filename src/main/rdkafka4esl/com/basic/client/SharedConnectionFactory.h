#ifndef RDKAFKA4ESL_COM_BASIC_CLIENT_SHAREDCONNECTIONFACTORY_H_
#define RDKAFKA4ESL_COM_BASIC_CLIENT_SHAREDCONNECTIONFACTORY_H_

#include <esl/com/basic/client/Connection.h>

#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace rdkafka4esl {
namespace com {
namespace basic {
namespace broker {
class Client;
}
namespace client {

class SharedConnection;

class SharedConnectionFactory {
public:
	// to call connectionRegister / connectionUnregister
	friend class Connection;

	SharedConnectionFactory(broker::Client& client, const std::vector<std::pair<std::string, std::string>>& topicSettings,
			const std::string& topicName, const std::string& key, std::int32_t partition);
	~SharedConnectionFactory();

	//called by SharedConnection
	void connectionRegister(SharedConnection* sharedConnection);
	void connectionUnregister(SharedConnection* sharedConnection);

	//called by broker::Client
	void release();

	std::unique_ptr<esl::com::basic::client::Connection> createConnection(std::shared_ptr<SharedConnectionFactory> sharedConnectionFactory) const;

private:
	broker::Client& client;
	const std::vector<std::pair<std::string, std::string>> kafkaSettings;
	const std::vector<std::pair<std::string, std::string>> topicParameters;
	const std::string topicName;
	const std::string key;
	const std::int32_t partition;

	std::mutex connectionWaitNotifyMutex;
	std::condition_variable connectionWaitCondVar;

	std::mutex sharedConnectionsMutex;
	std::set<SharedConnection*> sharedConnections;
	//std::set<std::shared_ptr<SharedConnection>> sharedConnections;
};

} /* namespace client */
} /* namespace basic */
} /* namespace com */
} /* namespace rdkafka4esl */

#endif /* RDKAFKA4ESL_COM_BASIC_CLIENT_SHAREDCONNECTIONFACTORY_H_ */
