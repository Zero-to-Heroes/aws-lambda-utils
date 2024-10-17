import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs';

export class Sqs {
	private readonly sqsClient: SQSClient;

	constructor() {
		this.sqsClient = new SQSClient({ region: 'us-west-2' });
	}

	public async sendMessageToQueue(message: SqsMessage, queueUrl: string): Promise<void> {
		try {
			const command = new SendMessageCommand({
				MessageBody: JSON.stringify(message),
				QueueUrl: queueUrl,
			});
			await this.sqsClient.send(command);
		} catch (err) {
			console.error('could not send message to queue', message, queueUrl, err);
		}
	}

	public async sendMessagesToQueue(messages: readonly SqsMessage[], queueUrl: string): Promise<void[]> {
		return Promise.all(messages.map(message => this.sendMessageToQueue(message, queueUrl)));
	}
}

// eslint-disable-next-line @typescript-eslint/no-empty-interface
export interface SqsMessage {}
