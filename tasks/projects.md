# Примерные темы проектов 

Общие требования:
- работающее решение с UI (web, через бот, командную строку)
- Docker/k8s
- код на Github
- Pipeline: непрерывный процесс получения данных, хранения, обработки 

1. **Онлайн обработка и классификация новостей**
Новости непрерывно загружаются из различных источников (tg, vk, web). На основе статистики получаются "горячие" новости за определенный период времени. Опция: классификация, извлечение данных (адреса, имена, и т.п.).

2. **Обработка биржевых индексов**
Нужно воспользоваться API как какой-нибудь платформы, научиться  определять аномалии или что-нибудь подобное.

3. **Работа с датчиками IoT**
Первая часть состоит из платформы, которая эмулирует датчики произвольной природы. Может настраиваться частота обновления, тип, вероятность "всплеска" обновлений. Показания записываются в MQTT, Kafka, ... Вторая часть системы обрабатывает показания, находит аномалии за заданные промежутки времени, хранит историю. В UI отображаются графики.  




