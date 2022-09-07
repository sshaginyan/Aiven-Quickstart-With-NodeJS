// We are importing our three npm packages
import { Kafka } from 'kafkajs';
import * as dotenv from 'dotenv';
import { faker } from '@faker-js/faker';

// Loads environment variables from .env file into process.env
dotenv.config();

// Creates an authenticated Kafka client
const kafka = new Kafka({
    brokers: [process.env.SERVICE_URI],
    ssl: {
	ca: [process.env.CA_CERTIFICATE],
	key: process.env.ACCESS_KEY,
	cert: process.env.ACCESS_CERTIFICATE
    }
});

// Creates and connects a producer to our Kafka cluster
const producer = kafka.producer();
await producer.connect();
// Creates a continuous stream of data
while(true) {

    const message = {
	key: `"${faker.datatype.uuid()}"`,
	value: JSON.stringify({
	    // Using faker to manufacture financial transactions data
	    timestamp: faker.date.recent(),
	    account: faker.finance.account(),
	    accountName: faker.finance.accountName(),
	    currency: faker.finance.currencyCode(),
	    card: faker.finance.creditCardIssuer(),
	    amount: faker.finance.amount()
	})
    };

    // Log our message before publishing to Kafka
    console.log(message);
    
    // Publishes messages to Kafka on a particular topic
    await producer.send({ topic: process.env.TOPIC, messages: [message] });
}

// Catches any uncaught exceptions and disconnect our producer
process.on('uncaughtException', async error => {
    console.error(error, 'Uncaught Exception thrown');
    await producer.disconnect();
    process.exit(1);
});

