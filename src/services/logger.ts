import { Context } from 'aws-lambda';

export const logger = {
	debugCallsBuffer: [] as (() => void)[],
	debug(message?: any, ...optionalParams: any[]) {
		this.debugCallsBuffer.push(() => console.debug(message, ...optionalParams));
	},
	log(message?: any, ...optionalParams: any[]) {
		console.log(message, ...optionalParams);
	},
	warn(message?: any, ...optionalParams: any[]) {
		console.warn(message, ...optionalParams);
	},
	error(message?: any, ...optionalParams: any[]) {
		console.error(message, ...optionalParams);
		console.log('Debug buffer');
		this.debugCallsBuffer.forEach(debugLogCall => debugLogCall());
	},
	clear() {
		this.debugCallsBuffer = [];
	},
};

export const logBeforeTimeout = (context: Context) => {
	const deadline = context.getRemainingTimeInMillis() - 100;
	const timeoutId = setTimeout(() => {
		logger.error('About to timeout');
	}, deadline);

	return () => clearTimeout(timeoutId);
};
