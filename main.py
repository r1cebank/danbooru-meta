import sys
import sqlite3
import json
import glob

ratingDict = {
    "s": ("s", "Safe", True),
    "q": ("q", "Questionable", False),
    "e": ("e", "Explicit", False)
}

COMMIT_INTERVAL = 10000


def insertTags(c, postId, tags):
    post_tags = []
    for tag in tags:
        ensureTag(c, tag)
        post_tags.append((postId, tag['id']))
    c.executemany(
        'INSERT OR IGNORE INTO post_tags (post_id, tag_id) VALUES (?, ?)', post_tags)


def ensureTag(c, tag):
    c.execute(
        'INSERT OR IGNORE INTO tags (tag_id, name, category) VALUES (?, ?, ?)', (tag['id'], tag['name'], tag['category']))


def ensureRating(c, rating):
    c.execute(
        'INSERT OR IGNORE INTO ratings (name, description, is_sfw) VALUES (?, ?, ?)', ratingDict[rating])


def createPost(c, post):
    c.execute(
        'INSERT OR IGNORE INTO posts (post_id, md5, rating, width, height, file_ext, file_size, source, pixiv_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)', (
            post['id'], post['md5'], post['rating'], post['image_width'], post[
                'image_height'], post['file_ext'], post['file_size'], post['source'], post['pixiv_id']
        )
    )


def ProcessLargeTextFile(db, filepath, current, total):
    lineNumber = 0
    with open(filepath, "r") as input_file:
        for line in input_file:
            lineNumber += 1
            post = json.loads(line)
            c = db.cursor()
            print(
                f'Processing post id: {post["id"]}, file: {filepath}, line number: {lineNumber} Progress: {current}/{total}')
            ensureRating(c, post['rating'])
            insertTags(c, post['id'], post['tags'])
            createPost(c, post)
            if lineNumber % COMMIT_INTERVAL == 0:
                db.commit()
                print(f'Commiting changes to disk line: {lineNumber}')


def updateStat(c):
    c.execute(
        'UPDATE stat set num_posts=(select count(rowid) as post_count from posts)')
    c.execute(
        'UPDATE stat set num_tags=(select count(rowid) as tag_count from tags)')
    c.execute(
        'UPDATE stat set num_ratings=(select count(rowid) as rating_count from ratings)')


def createIndex(c):
    c.execute('DROP INDEX IF EXISTS pt_post_id')
    c.execute('DROP INDEX IF EXISTS pt_tag_id')
    c.execute('DROP INDEX IF EXISTS p_post_id')
    c.execute('DROP INDEX IF EXISTS t_tag_id')
    c.execute('CREATE INDEX pt_post_id ON post_tags (post_id)')
    c.execute('CREATE INDEX pt_tag_id ON post_tags (tag_id)')
    c.execute('CREATE INDEX p_post_id ON posts (post_id)')
    c.execute('CREATE INDEX t_tag_id ON tags (tag_id)')


def main():
    currentFile = 0
    db = sqlite3.connect(sys.argv[2])
    meta_files = glob.glob(f'{sys.argv[1]}/**/*')
    total_files = meta_files.__len__()
    print(f'Total metafiles to import: {total_files}')
    for file in meta_files:
        currentFile += 1
        print(f'Processing: {file} {currentFile}/{total_files}')
        ProcessLargeTextFile(db, file, currentFile, total_files)
    print('Finished importing in bulk, updating stats...')
    c = db.cursor()
    updateStat(c)
    print('Creating index....this may take a while')
    createIndex(c)
    db.commit()


if __name__ == "__main__":
    main()
