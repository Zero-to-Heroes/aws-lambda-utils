import { Context } from 'aws-lambda';

export const logger: Logger = {
	debugCallsBuffer: [] as { timestamp: number; log: () => void }[],
	debug(message?: any, ...optionalParams: any[]) {
		const date = new Date().toISOString();
		this.debugCallsBuffer.push({
			timestamp: date,
			log: () => console.debug(date, message, ...optionalParams),
		});
	},
	log(message?: any, ...optionalParams: any[]) {
		console.log(message, ...optionalParams);
	},
	warn(message?: any, ...optionalParams: any[]) {
		console.warn(message, ...optionalParams);
	},
	error(message?: any, ...optionalParams: any[]) {
		this.dumpBuffer();
		console.error(message, ...optionalParams);
	},
	clear() {
		this.debugCallsBuffer = [];
	},
	dumpBuffer() {
		console.log('Debug buffer');
		(this.debugCallsBuffer as { timestamp: number; log: () => void }[])
			.sort((a, b) => a.timestamp - b.timestamp)
			.forEach(debugLogCall => debugLogCall.log());
		console.log('End debug buffer');
	},
};

export const logBeforeTimeout = (context: Context) => {
	const deadline = context.getRemainingTimeInMillis() - 200;
	logger.debug('Will timeout in', deadline, 'ms');
	const timeoutId = setTimeout(() => {
		logger.error('About to timeout');
	}, deadline);
	logger.clear();
	return () => clearTimeout(timeoutId);
};

interface Logger {
	debugCallsBuffer: { timestamp: number; log: () => void }[];
	debug: (message?: any, ...optionalParams: any[]) => void;
	log: (message?: any, ...optionalParams: any[]) => void;
	warn: (message?: any, ...optionalParams: any[]) => void;
	error: (message?: any, ...optionalParams: any[]) => void;
	clear: () => void;
	dumpBuffer: () => void;
}
