import { database } from '$lib/database'

export { load }

// For the page's initial data, we use SvelteKit's built-in data fetching mechanism
// instead of Telefunc, see https://telefunc.com/initial-data
const load: import('./$types').PageServerLoad = async () => {
  const { value } = database
  return {
    value,
  }
}
