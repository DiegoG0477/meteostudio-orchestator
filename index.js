import { connect } from 'amqplib';
import dotenv from 'dotenv';

dotenv.config();

const sendToExchange = async (exchange, msg, channel) => {
    channel.publish(exchange, '', Buffer.from(msg.content.toString()));
}

const assignExchange = (msg) => {
    const ENTERPRISE_EXCHANGE = process.env.ENTERPRISE_EXCHANGE;
    const METEOROLOGICAL_EXCHANGE = process.env.METEOROLOGICAL_EXCHANGE;
    const PAYMENT_EXCHANGE = process.env.PAYMENT_EXCHANGE;

    const data = JSON.parse(msg.content.toString());

    const queue = data.event.split('.')[0];
    
    switch (queue) {
        case 'enterprise':
            return String(ENTERPRISE_EXCHANGE);
        case 'meteorological':
            return String(METEOROLOGICAL_EXCHANGE);
        case 'payment':
            return String(PAYMENT_EXCHANGE);
        default:
            break;
    }
}

async function consumeAndSend() {
    try {
        const USERNAME = process.env.AMQP_USERNAME;
        const PASSWORD = encodeURIComponent(process.env.AMQP_PASSWORD);
        const HOSTNAME = process.env.AMQP_HOSTNAME;
        const PORT = process.env.AMQP_PORT;
        const QUEUE_NAME = process.env.AMQP_QUEUE;

        const connection = await connect(`amqp://${USERNAME}:${PASSWORD}@${HOSTNAME}:${PORT}`);
        const channel = await connection.createChannel();

        const queueName = QUEUE_NAME;

        console.log(
            `Conectado al servidor RabbitMQ. Esperando mensajes en la cola ${queueName}.`
        );

        // Asegurarse de que la cola existe
        await channel.assertQueue(queueName, {
            durable: true,
            arguments: { "x-queue-type": "quorum" },
        });

        // Suscripción a la cola
        channel.consume(queueName, async (msg) => {
            // console.log(`Mensaje recibido: ${msg.content.toString()}`);

            // Envía un ack para confirmar que el mensaje ha sido procesado
            const exchange = assignExchange(msg);
            await sendToExchange(exchange, msg, channel);
            
            channel.ack(msg);
            console.log("Mensaje procesado");
        });
    } catch (error) {
        console.error("Error al conectar o consumir la cola:", error);
    }
}

consumeAndSend();