import { SQS } from '@aws-sdk/client-sqs';

export class Sqs {
	private readonly sqs: SQS;

	constructor() {
		this.sqs = new SQS({ apiVersion: '2012-11-05', region: 'us-west-2' });
	}

	public async sendMessageToQueue(message: SqsMessage, queueUrl: string): Promise<void> {
		return new Promise<void>(resolve => {
			this.sqs.sendMessage(
				{
					MessageBody: JSON.stringify(message),
					QueueUrl: queueUrl,
				},
				(err, data) => {
					if (err) {
						console.error('could not send message to queue', message, queueUrl, err);
						resolve();
						return;
					}
					resolve();
				},
			);
		});
	}

	public async sendMessagesToQueue(messages: readonly SqsMessage[], queueUrl: string): Promise<void[]> {
		return Promise.all(messages.map(message => this.sendMessageToQueue(message, queueUrl)));
	}
}

// eslint-disable-next-line @typescript-eslint/no-empty-interface
export interface SqsMessage {}
