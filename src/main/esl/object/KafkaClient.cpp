#include <esl/object/KafkaClient.h>
#include <esl/system/Stacktrace.h>

#include <rdkafka4esl/com/basic/broker/Client.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace esl {
inline namespace v1_6 {
namespace object {

KafkaClient::Settings::Settings(const std::vector<std::pair<std::string, std::string>>& settings) {
	bool hasGroupId = false;

	for(auto& setting : settings) {
		if(setting.first.size() > 6 && setting.first.substr(0, 6) == "kafka.") {
			std::string kafkaKey = setting.first.substr(6);
			if(kafkaKey == "group.id") {
				hasGroupId = true;
			}
			kafkaSettings.emplace_back(kafkaKey, setting.second);
		}
		else {
			throw std::runtime_error("Invalid parameter key \"" + setting.first + "\" for MemBuffer appender");
		}
	}

	if(!hasGroupId) {
		throw esl::system::Stacktrace::add(std::runtime_error("Value \"kafka.group.id\" not specified."));
	}
}

KafkaClient::KafkaClient(const Settings& settings)
: object(new rdkafka4esl::com::basic::broker::Client(settings))
{ }

std::unique_ptr<Object> KafkaClient::create(const std::vector<std::pair<std::string, std::string>>& settings) {
	return std::unique_ptr<Object>(new KafkaClient(Settings(settings)));
}

} /* namespace object */
} /* inline namespace v1_6 */
} /* namespace esl */
