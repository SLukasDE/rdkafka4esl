#ifndef ESL_OBJECT_KAFKACLIENT_H_
#define ESL_OBJECT_KAFKACLIENT_H_

#include <esl/object/Object.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace esl {
inline namespace v1_6 {
namespace object {

class KafkaClient : public Object {
public:
	struct Settings {
		Settings(const std::vector<std::pair<std::string, std::string>>& settings);
		std::vector<std::pair<std::string, std::string>> kafkaSettings;
	};

	KafkaClient(const Settings& settings);

	static std::unique_ptr<Object> create(const std::vector<std::pair<std::string, std::string>>& settings);

private:
	std::unique_ptr<Object> object;
};

} /* namespace object */
} /* inline namespace v1_6 */
} /* namespace esl */

#endif /* ESL_OBJECT_KAFKACLIENT_H_ */
