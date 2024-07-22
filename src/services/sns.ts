import { SNS } from '@aws-sdk/client-sns';

export class Sns {
	private readonly sns: SNS;

	constructor() {
		this.sns = new SNS({ region: 'us-west-2' });
	}

	public async notify(topic: string, message: string) {
		await this.sns.publish({
			Message: message,
			TopicArn: topic,
		});
	}

	public async notifyBgPerfectGame(review: any) {
		const topic = process.env.BG_PERFECT_GAME_SNS_TOPIC;
		await this.sns.publish({
			Message: JSON.stringify(review),
			TopicArn: topic,
		});
	}
}
