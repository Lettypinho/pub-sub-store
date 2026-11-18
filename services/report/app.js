const RabbitMQService = require('./rabbitmq-service')
const path = require('path')

require('dotenv').config({ path: path.resolve(__dirname, '.env') })

var report = {}
async function updateReport(products) {
    for(let product of products) {
        if(!product.name) {
            continue
        } else if(!report[product.name]) {
            report[product.name] = 1;
        } else {
            report[product.name]++;
        }
    }

}

async function printReport() {
    for (const [key, value] of Object.entries(report)) {
        console.log(`${key} = ${value} sales`);
      }
}


async function consume() {
    const queueName = process.env.RABBITMQ_QUEUE_NAME;
    const rabbitMQ = await RabbitMQService.getInstance();

    await rabbitMQ.consume(queueName, async (message) => {
        if (!message) return;

        try {
            // Converte o conteúdo da mensagem em um objeto JavaScript.
            const order = JSON.parse(message.content.toString('utf8'));

            if (!Array.isArray(order?.products)) {
                throw new Error('A mensagem precisa conter uma lista de produtos.');
            }

            await updateReport(order.products);
            await printReport();
        } catch (error) {
            console.error('Erro ao processar pedido:', error.message);
        }
    });
}

consume().catch((error) => {
    console.error('Erro ao iniciar o serviço de relatórios:', error.message);
    process.exitCode = 1;
});
