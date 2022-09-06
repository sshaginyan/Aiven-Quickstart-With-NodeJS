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

// Creates a continous stream of generated data
while(true) {
    const messages = [];
    // Creates up to 500 batched financial transactions at a given time
    for(let index = 0; index < Math.floor(Math.random() * 500) + 1; index++) {
	// Populates array `messages` with fake data using faker
	messages.push({
	    key: `"${faker.datatype.uuid()}"`,
	    value: JSON.stringify({
		timestamp: faker.date.recent(),
		account: faker.finance.account(),
		accountName: faker.finance.accountName(),
		currency: faker.finance.currencyCode(),
		card: faker.finance.creditCardIssuer(),
		amount: faker.finance.amount()
	    })
	});
    }
    // Publishes messages to Kafka on a particular topic
    await producer.send({ topic: process.env.TOPIC, messages });
}

// Catches any uncaught exceptions and disconnect our producer
process.on('uncaughtException', async error => {
    console.error(error, 'Uncaught Exception thrown');
    await producer.disconnect();
    process.exit(1);
});

