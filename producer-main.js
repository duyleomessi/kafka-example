const express = require('express')
const bodyParser = require('body-parser')
const {producer} = require('./producer')
const config = require('./config')

const app = express()

app.use(bodyParser.json())
app.use(bodyParser.urlencoded({extended: true}))

producer.on('ready', () => {
    console.log(`Producer is ready`)
})

app.post('/message', (req, res) => {
    const data = req.body
    // process message
    data.message.content += ` ${parseInt(Math.random() * 100, 10)}`
    const message = Buffer.from(JSON.stringify(data))
    console.log(data)
    console.log(`producer ready`)
    const currentTime = Date.now()
    console.log(currentTime)
    try {
        producer.produce(config.KafkaTopic, null, message, null, currentTime, (err, offset) => {
            if (err) {
                const errMsg = `Error in sending message from producer: ${err}`
                console.log(errMsg)
                return res.status(500).json({message: errMsg})
            } else {
                console.log(offset)
                return res.status(200).json({offset: offset})
            }
        })
    } catch (err) {
        const errMsg = `A problem occurred when sending our message: ${err}`
        console.error(errMsg)
        return res.status(500).json({message: errMsg})
    }
})


const port = 3004

app.listen(port, () => {
    console.log('Listen on port ' + port)
})