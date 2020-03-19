const kafka = require('node-rdkafka')
const async = require('async')
const config = require('./config')

const topics = [config.KafkaTopic]

const consumerConfig = {
    'group.id': async.groupId,
    'metadata.broker.list': config.KafkaHost,
    // offset commit callback
    'offset_commit_cb': (err, topicPartitions) => {
        if (err) {
            console.error(err)
        } else {
            console.log(`Commit at ${JSON.stringify(topicPartitions)}`)
        }
    },
    // rebalance callback
    'rebalance_cb': function (err, assignments) {
        console.log(`assignments: ${JSON.stringify(assignments)}`)
        if (err.code === kafka.CODES.ERRORS.ERR__ASSIGN_PARTITIONS) {
            console.log(`kafka.CODES.ERRORS.ERR__ASSIGN_PARTITIONS`)
            consumer.assign(assignments)
        } else if (err.code === kafka.CODES.ERRORS.ERR__REVOKE_PARTITIONS) {
            console.log(`kafka.CODES.ERRORS.ERR__REVOKE_PARTITIONS`)
            console.log(`paused: ${paused}`)
            if (paused) {
                consumer.resume(assignments)
                paused = false
            }
            msgQueue.remove((d, p) => {
                console.log(`msgQueue remove `)
                return true
            })
            consumer.unassign()
            // console.log(`kafka.CODES.ERRORS.ERR__REVOKE_PARTITIONS`)
        } else {
            console.error(`REBALANCE ERROR: ${err}`)
        }
    }
}

const topicConfig = {}

let paused = false

const consumer = new kafka.KafkaConsumer(consumerConfig, topicConfig)

consumer.connect({timeout: 1000}, (err) => {
    if (err) {
        console.log(`Error connecting to Kafka broker: ${err}`)
        process.exit(-1)
    }
    console.log(`Connected to Kafka broker`)
})

consumer.on('ready', (arg) => {
    console.log(`Consumer ready: ${JSON.stringify(arg)}`)
    consumer.subscribe(topics)
    // start consuming messages
    consumer.consume()
})

consumer.on('data', (data) => {
    // console.log(JSON.parse(data.value.toString()))
    const producerTimestamp = data.timestamp
    const consumerTimestamp = Date.now()
    console.log(`Queue length: ` + msgQueue.length())
    console.log(`distance: ${consumerTimestamp - producerTimestamp}`)
    if (consumerTimestamp - producerTimestamp <= 10000) {
        msgQueue.push(data)
    }

    if (msgQueue.length() > config.maxQueueSize) {
        consumer.pause(consumer.assignments())
        paused = true
        console.log(`Queue is full of shit. Pause the consumer`)
    }
})

const msgQueue = async.queue(async (data, done) => {
    console.debug(`Handling received: ${msgQueue.length()}`)
    await handleCB(data, onData)
    done()
}, config.maxParallelHandles)

const handleCB = async (data, handler) => {
    await handler(data)
}

const onData = async (data) => {
    return setTimeout(async () => {
        return Promise.resolve()
    }, 0.2 * 1000)
}

msgQueue.drain(function (err) {
    console.log(`DRAIN FUNCTION`)

    if (err) {
        console.log(`Drain error: ${err}`)
    }

    if (paused) {
        console.log(`Drain all of queue.`)
        consumer.resume(consumer.assignments())
        paused = false
    }
})

consumer.on('disconnected', (arg) => {
    console.log(`Consumer disconnected. ${JSON.stringify(arg)}`)
})