import { loadQuartzConfig, loadQuartzLayout } from "./quartz/plugins/loader/config-loader"
import { componentRegistry } from "./quartz/components/registry"

componentRegistry.setOptionOverrides("@quartz-community/explorer", {
	sortFn: (a, b) => {
		return a.displayName.localeCompare(b.displayName, undefined, {
			numeric: true,
			sensitivity: "base",
		})
	},
})

componentRegistry.setOptionOverrides("@quartz-community/folder-page", {
	sort: (a, b) => {
		const aTitle = a.frontmatter?.title ?? ""
		const bTitle = b.frontmatter?.title ?? ""
		return aTitle.localeCompare(bTitle, undefined, {
			numeric: true,
			sensitivity: "base",
		})
	},
})

const config = await loadQuartzConfig()
export default config
export const layout = await loadQuartzLayout()
