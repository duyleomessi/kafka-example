const kafka = require('node-rdkafka')
const config = require('./config')

const producer = new kafka.HighLevelProducer({
    'metadata.broker.list': config.KafkaHost
})

producer.connect()



producer.on('event.error', (err) => {
    console.log(`Error from producer: ${err}`)
})

module.exports = Object.assign({}, {producer})