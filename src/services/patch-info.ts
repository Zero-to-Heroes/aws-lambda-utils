import { http } from './utils';

export const getPatchInfos = async (): Promise<PatchesConfig> => {
	const patchInfo = await http(`https://static.zerotoheroes.com/hearthstone/data/patches.json`);
	const structuredPatch = JSON.parse(patchInfo);
	return structuredPatch;
};

export const getLastBattlegroundsPatch = async (): Promise<PatchInfo> => {
	const config = await getPatchInfos();
	const patchNumber = config.currentBattlegroundsMetaPatch;
	return config.patches.find(patch => patch.number === patchNumber);
};

export const getLastConstructedPatch = async (): Promise<PatchInfo> => {
	const config = await getPatchInfos();
	const patchNumber = config.currentConstructedMetaPatch;
	return config.patches.find(patch => patch.number === patchNumber);
};

export const getLastArenaPatch = async (): Promise<PatchInfo> => {
	const config = await getPatchInfos();
	const patchNumber = config.currentArenaMetaPatch;
	return config.patches.find(patch => patch.number === patchNumber);
};

export interface PatchesConfig {
	readonly patches: readonly PatchInfo[];
	readonly currentConstructedMetaPatch: number;
	readonly currentBattlegroundsMetaPatch: number;
	readonly currentDuelsMetaPatch: number;
	readonly currentArenaMetaPatch: number;
	readonly currentMercenariesMetaPatch: number;
}

export interface PatchInfo {
	readonly number: number;
	readonly version: string;
	readonly name: string;
	readonly date: string;
	readonly hasNewBuildNumber?: boolean;
}
