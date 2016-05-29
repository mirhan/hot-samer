import cv2
import sys, os
from template import generate_html_tmp

# Get user supplied values
# imagePath = sys.argv[1]
CASCADE_PATH = r'/Users/hanchang/Developer/FaceDetect-master/haarcascade_frontalface_default.xml'
# # Create the haar cascade
# faceCascade = cv2.CascadeClassifier(CASCADE_PATH)

# # Read the image
# image = cv2.imread(imagePath)
# gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

# # Detect faces in the image
# faces = faceCascade.detectMultiScale(
#     gray,
#     scaleFactor=1.1,
#     minNeighbors=5,
#     minSize=(30, 30),
#     flags = cv2.cv.CV_HAAR_SCALE_IMAGE
# )

# print "Found {0} faces!".format(len(faces))

# # Draw a rectangle around the faces
# for (x, y, w, h) in faces:
#     cv2.rectangle(image, (x, y), (x+w, y+h), (0, 255, 0), 2)

# cv2.imshow("Faces found", image)
# cv2.waitKey(0)


def is_any_faces(imagePath):
    image = cv2.imread(imagePath)
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

    # Create the haar cascade
    faceCascade = cv2.CascadeClassifier(CASCADE_PATH)

    faces = faceCascade.detectMultiScale(
        gray,
        scaleFactor=1.1,
        minNeighbors=5,
        minSize=(30, 30),
        flags=cv2.cv.CV_HAAR_SCALE_IMAGE)

    return len(faces) > 0


def detect_faces_in_dir(pic_dir):
    a = []
    for subdir, dirs, files in os.walk(pic_dir):
        for file in files:
            filename = os.path.join(subdir, file)
            extension = (os.path.splitext(filename)[1]).lower()
            if extension == 'png' or extension == '.jpg':
                # print '{0} exsits: {1}'.format(filename, is_any_faces(filename))
                if is_any_faces(filename):
                    a.append(filename)
                    print len(a)
                    if len(a) >= 10:
                        return a
                # yield filename, is_any_faces(filename)

PIC_DIR = r'/Users/hanchang/Pictures/samer_pics/old'
if __name__ == '__main__':
    # imagePath = sys.argv[1]
    # print is_any_faces(imagePath)
    # pic_dir = sys.argv[1]
    pics = detect_faces_in_dir(PIC_DIR)
    # for pic_path in detect_faces_in_dir(PIC_DIR):
    generate_html_tmp(pics)

    # print detect_faces_in_dir(PIC_DIR)
