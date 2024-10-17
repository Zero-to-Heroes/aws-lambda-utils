import { PublishCommand, SNSClient } from '@aws-sdk/client-sns';

export class Sns {
	private readonly sns: SNSClient;

	constructor() {
		this.sns = new SNSClient({ region: 'us-west-2' });
	}

	public async notify(topic: string, message: string) {
		const command = new PublishCommand({
			Message: message,
			TopicArn: topic,
		});
		await this.sns.send(command);
	}

	public async notifyBgPerfectGame(review: any) {
		const topic = process.env.BG_PERFECT_GAME_SNS_TOPIC;
		const command = new PublishCommand({
			Message: JSON.stringify(review),
			TopicArn: topic,
		});
		await this.sns.send(command);
	}
}
