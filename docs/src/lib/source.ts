import { docs } from "fumadocs-mdx:collections/server";
import { loader } from "fumadocs-core/source";
import { lucideIconsPlugin } from "fumadocs-core/source/lucide-icons";

const metaFiles = import.meta.glob("../../content/docs/**/_meta.json", {
	eager: true,
	import: "default",
});

const folderOrderByPath = Object.fromEntries(
	Object.entries(metaFiles as Record<string, { pages?: string[] }>)
		.map(([path, meta]) => {
			const normalizedPath = path.replace(/\\/g, "/");
			const folderPath = normalizedPath
				.replace(/^.*\/content\/docs\//, "")
				.replace(/^\.?\//, "")
				.replace(/\/_meta\.json$/, "")
				.replace(/^_meta\.json$/, "");
			const pages = Array.isArray(meta.pages)
				? meta.pages.map((entry) => entry.toLowerCase())
				: [];
			return [folderPath, pages];
		}),
);

const docsNavOrderPlugin = {
	name: "docs-sidebar-order",
	enforce: "pre" as const,
	transformPageTree: {
		root: (root: any) => {
			root.children = sortSidebarByOrder(root.children);
			return root;
		},
	},
};

function sortSidebarByOrder(
	children: Array<{
		type: string;
		name?: unknown;
		index?: { url?: unknown };
		url?: unknown;
		children?: Array<{ type: string; name?: unknown; children?: any }>;
	}>,
	parentPath = "",
) {
	const order = new Map(
		folderOrderByPath[parentPath]?.map((name, index) => [name, index]) ?? [],
	);

	const folderNodes = children.filter((node) => node.type === "folder");

	folderNodes.forEach((folder) => {
		if (Array.isArray(folder.children)) {
			const folderPath = getFolderPath(folder, parentPath);
			folder.children = sortSidebarByOrder(folder.children, folderPath);
		}
	});

	children.sort((a, b) => {
		const firstName = getNodeSortKey(a);
		const secondName = getNodeSortKey(b);
		const first = order.get(firstName);
		const second = order.get(secondName);

		if (first !== undefined || second !== undefined) {
			if (first === undefined) return 1;
			if (second === undefined) return -1;
			if (first === second) return 0;
			return first - second;
		}

		if (firstName && secondName) {
			return firstName.localeCompare(secondName);
		}
		if (!firstName) return 1;
		if (!secondName) return -1;
		return 0;
	});

	const pageNodes = children.filter((node) => node.type !== "folder");
	const sortedFolderNodes = children.filter((node) => node.type === "folder");

	return [...pageNodes, ...sortedFolderNodes];
}

function getFolderPath(
	folder: { index?: { url?: unknown } },
	parentPath = "",
): string {
	const indexUrl = folder.index?.url;
	if (typeof indexUrl === "string") {
		return pathFromUrl(indexUrl);
	}

	const folderSlug = getNodeSortKey(folder);
	return parentPath ? `${parentPath}/${folderSlug}` : folderSlug;
}

function pathFromUrl(url: string): string {
	const normalized = url.replace(/\/+$/, "");
	if (normalized === "/docs") return "";
	return normalized.startsWith("/docs/")
		? normalized.slice(6)
		: normalized.replace(/^\//, "");
}

function getNodeSortKey(node: {
	name?: unknown;
	url?: unknown;
	index?: { url?: unknown };
}): string {
	if (typeof node.index?.url === "string") {
		const indexPath = pathFromUrl(node.index.url);
		if (!indexPath) return "index";
		return indexPath.split("/").at(-1) ?? "";
	}

	if (typeof node.url === "string") {
		const normalized = node.url.replace(/\/+$/, "");
		const path = pathFromUrl(normalized);
		if (!path) return "index";
		return path.split("/").at(-1) ?? "";
	}

	return typeof node.name === "string" ? node.name.toLowerCase() : "";
}

export const source = loader({
	source: docs.toFumadocsSource(),
	baseUrl: "/docs",
	plugins: [docsNavOrderPlugin, lucideIconsPlugin()],
});
