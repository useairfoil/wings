if [[ "${GITHUB_REF:-}" != "refs/tags/"* ]]; then
    echo "The GITHUB_REF variable does not contain a tag reference."
    exit 0
fi

version="${GITHUB_REF#refs/tags/}"

echo "Parsing version: ${version}"

major=$(semver get major "${version}")
minor=$(semver get minor "${version}")
patch=$(semver get patch "${version}")
prerel=$(semver get prerel "${version}")

echo "major=${major}"
echo "minor=${minor}"
echo "patch=${patch}"
echo "prerel=${prerel}"

if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    echo "Writing output to ${GITHUB_OUTPUT}"
    {
        echo "major=${major}"
        echo "minor=${minor}"
        echo "patch=${patch}"
        echo "prerel=${prerel}"
    } >> "$GITHUB_OUTPUT"
fi
