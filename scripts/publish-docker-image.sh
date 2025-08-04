function dry_run() {
    if [[ "${DRY_RUN:-false}" == "true" ]]; then
        echo "[dry-run] $*"
    else
        "$@"
    fi
}

if [[ -z "${IMAGE_NAME:-}" ]]; then
    echo "IMAGE_NAME is not set"
    exit 1
fi

if [[ -z "${IMAGE_VERSION_MAJOR:-}" ]]; then
    echo "IMAGE_VERSION_MAJOR is not set"
    exit 1
fi

if [[ -z "${IMAGE_VERSION_MINOR:-}" ]]; then
    echo "IMAGE_VERSION_MINOR is not set"
    exit 1
fi

if [[ -z "${IMAGE_VERSION_PATCH:-}" ]]; then
    echo "IMAGE_VERSION_PATCH is not set"
    exit 1
fi

if ! [ -f "${IMAGE_ARCHIVE_x86_64:-}" ]; then
    echo "IMAGE_ARCHIVE_x86_64 image does not exist"
    exit 1
fi

if ! [ -f "${IMAGE_ARCHIVE_aarch64:-}" ]; then
    echo "IMAGE_ARCHIVE_aarch64 image does not exist"
    exit 1
fi

echo '{"default": [{"type": "insecureAcceptAnything"}]}' > /tmp/policy.json

echo "::group::Logging in to Quay.io"
echo "::add-mask::${QUAY_USERNAME}"
echo "::add-mask::${QUAY_PASSWORD}"
dry_run skopeo login -u="${QUAY_USERNAME}" -p="${QUAY_PASSWORD}" quay.io
echo "::endgroup::"

base="quay.io/airfoil/${IMAGE_NAME}"
version="${IMAGE_VERSION_MAJOR}.${IMAGE_VERSION_MINOR}.${IMAGE_VERSION_PATCH}"
if ! [[ -z "${IMAGE_VERSION_PREREL:-}" ]]; then
    version="${version}-${IMAGE_VERSION_PREREL}"
fi

echo "::group::Publishing image ${IMAGE_NAME}:${version}"
dry_run skopeo copy "docker-archive:${IMAGE_ARCHIVE_x86_64}" "docker://${base}:${version}-x86_64"
dry_run skopeo copy "docker-archive:${IMAGE_ARCHIVE_aarch64}" "docker://${base}:${version}-aarch64"
echo "::endgroup::"

images=("${base}:${version}-x86_64" "${base}:${version}-aarch64")

echo "::group::Create manifest ${base}:${version}"
manifest="${base}:${version}"
dry_run buildah manifest create "${manifest}" "${images[@]}"
echo "::endgroup::"

if ! [[ -z "${IMAGE_VERSION_PREREL:-}" ]]; then
    tag="${IMAGE_VERSION_MAJOR}.${IMAGE_VERSION_MINOR}.${IMAGE_VERSION_PATCH}-${IMAGE_VERSION_PREREL}"
    echo "::group::Push manifest ${base}:${tag}"
    dry_run buildah manifest push --all "${manifest}" "docker://${base}:${tag}"
    echo "::endgroup::"
fi

tag="${IMAGE_VERSION_MAJOR}.${IMAGE_VERSION_MINOR}.${IMAGE_VERSION_PATCH}"
echo "::group::Push manifest ${base}:${tag}"
dry_run buildah manifest push --all "${manifest}" "docker://${base}:${tag}"
echo "::endgroup::"

tag="${IMAGE_VERSION_MAJOR}.${IMAGE_VERSION_MINOR}"
echo "::group::Push manifest ${base}:${tag}"
dry_run buildah manifest push --all "${manifest}" "docker://${base}:${tag}"
echo "::endgroup::"

tag="${IMAGE_VERSION_MAJOR}"
echo "::group::Push manifest ${base}:${tag}"
dry_run buildah manifest push --all "${manifest}" "docker://${base}:${tag}"
echo "::endgroup::"

tag="latest"
echo "::group::Push manifest ${base}:${tag}"
dry_run buildah manifest push --all "${manifest}" "docker://${base}:${tag}"
echo "::endgroup::"
