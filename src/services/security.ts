import {
	GetSecretValueCommand,
	GetSecretValueCommandOutput,
	GetSecretValueRequest,
	SecretsManagerClient,
} from '@aws-sdk/client-secrets-manager';
import { JwtPayload, decode, verify } from 'jsonwebtoken';
import fetch from 'node-fetch';

const secretsManager = new SecretsManagerClient({ region: 'us-west-2' });

export const validateOwToken = async (token: string): Promise<TokenValidationResult> => {
	if (!token?.length) {
		return null;
	}

	const decoded: JwtPayload = decode(token) as JwtPayload;
	// Check if JWT token is expired
	if (decoded.exp * 1000 < Date.now()) {
		return null;
	}

	const response = await fetch(
		`https://accounts.overwolf.com/tokens/short-lived/users/profile?extensionId=lnknbakkpommmjjdnelmfbjjdbocfpnpbkijjnob`,
		{
			method: 'GET',
			headers: {
				'Authorization': `Bearer ${token}`,
			},
		},
	);
	const validationResult: TokenValidationResult = (await response.json()) as TokenValidationResult;
	return validationResult;
};

export const validateFirestoneToken = async (token: string): Promise<TokenValidationResult | null> => {
	if (!token?.length) {
		return null;
	}

	const secretRequest: GetSecretValueRequest = {
		SecretId: 'sso',
	};
	const secret: SecretInfo = await getSecret(secretRequest);
	const payload: JwtPayload = verify(token, secret.fsJwtTokenKey) as JwtPayload;
	// Check if token is expired
	if (payload.exp * 1000 < Date.now()) {
		return null;
	}

	return {
		username: payload.userName,
	};
};

export interface TokenValidationResult {
	readonly username: string;
	readonly nickname?: string;
}

const getSecret = async (secretRequest: GetSecretValueRequest): Promise<SecretInfo> => {
	try {
		const command = new GetSecretValueCommand(secretRequest);
		const data: GetSecretValueCommandOutput = await secretsManager.send(command);
		const secretInfo: SecretInfo = JSON.parse(data.SecretString || '{}');
		return secretInfo;
	} catch (err) {
		console.error('could not get secret value', err);
		return null;
	}
};

interface SecretInfo {
	readonly clientId: string;
	readonly clientSecret: string;
	readonly fsJwtTokenKey: string;
}
